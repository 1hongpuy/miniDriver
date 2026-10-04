#!/usr/bin/env python3
"""Correlate a placement-smoke runs.csv with DataNode completion logs."""

import argparse
import csv
import math
import re
import subprocess
from collections import defaultdict
from pathlib import Path


DEPLOYMENTS = ("datanode-1", "datanode-2", "datanode-3")
METRICS = (
    "total_ms",
    "admission_wait_us",
    "body_receive_us",
    "disk_queue_wait_us",
    "disk_write_wall_us",
    "pwrite_us",
    "write_ready_wait_us",
    "durability_queue_wait_us",
    "durability_batch_formation_us",
    "data_sync_us",
    "index_write_us",
    "index_mutex_wait_us",
    "index_batch_build_us",
    "replica_wait_us",
    "durability_worker_busy_wait_us",
    "durability_callback_dispatch_us",
    "completion_wakeup_us",
    "group_wait_us",
    "group_commit_us",
    "group_batch_items",
    "gateway_commit_us",
    "post_commit_us",
)
TOKEN = re.compile(r"([A-Za-z0-9_]+)=([^\s]+)")


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return ordered[index]


def successful_sessions(runs_path: Path) -> set[str]:
    with runs_path.open(newline="", encoding="utf-8") as stream:
        rows = csv.DictReader(stream)
        return {
            row["upload_session_id"]
            for row in rows
            if row.get("upload_ok") == "true"
            and row.get("download_ok") == "true"
            and not row.get("error")
            and row.get("upload_session_id")
        }


def datanode_log_file(deployment: str) -> str:
    return "/var/lib/minidriver/datanode/logs/" + deployment.replace("datanode-", "datanode-dn-") + ".log"


def kubectl_logs(deployment: str) -> str:
    result = subprocess.run(
        [
            "k3s", "kubectl", "-n", "minidriver", "exec",
            f"deployment/{deployment}", "--", "cat", datanode_log_file(deployment),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"{deployment}: {result.stderr.strip()}")
    return result.stdout


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "report_dir", nargs="?", type=Path,
        default=Path("/var/lib/minidriver-bench/placement-smoke"),
    )
    parser.add_argument(
        "--since", default="",
        help="deprecated compatibility option; DataNode completion logs are read from their persistent files",
    )
    args = parser.parse_args()

    report_dir = args.report_dir
    sessions = successful_sessions(report_dir / "runs.csv")
    if not sessions:
        raise SystemExit("no successful sessions found in runs.csv")

    evidence_dir = report_dir / "datanode-evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    placements: dict[str, set[str]] = defaultdict(set)

    print(f"report_dir={report_dir}")
    print(f"successful_sessions={len(sessions)} source=persistent_datanode_log_files")
    for deployment in DEPLOYMENTS:
        log = kubectl_logs(deployment)
        matches: list[str] = []
        for line in log.splitlines():
            if "event=chunk_complete" not in line:
                continue
            tokens = dict(TOKEN.findall(line))
            if tokens.get("session") not in sessions:
                continue
            matches.append(line)
            grouped[(deployment, tokens.get("role", "unknown"))].append(tokens)
            placements[tokens["session"]].add(deployment)
        (evidence_dir / f"{deployment}.log").write_text(
            "\n".join(matches) + ("\n" if matches else ""), encoding="utf-8"
        )
        print(f"{deployment}_matched_completions={len(matches)}")

    if not grouped:
        raise SystemExit(
            "no matching DataNode completion logs; the matching log records may have rotated"
        )

    pairs: dict[str, int] = defaultdict(int)
    incomplete_sessions = 0
    for session in sessions:
        nodes = sorted(placements.get(session, set()))
        if len(nodes) == 2:
            pairs[" + ".join(nodes)] += 1
        else:
            incomplete_sessions += 1
    print("rf2_pairs:")
    for pair, count in sorted(pairs.items()):
        print(f"  {pair}: sessions={count}")
    if incomplete_sessions:
        print(f"  incomplete_log_pairs={incomplete_sessions}")

    timeline_path = evidence_dir / "session-timeline.csv"
    timeline_fields = (
        "session", "index", "request_id", "chunk", "chunk_id", "role", "http_status",
        "durable_sequence", "group_batch_items", "sync_timing_owner", *METRICS,
    )
    with timeline_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=("placement_pair", "deployment", *timeline_fields))
        writer.writeheader()
        for (deployment, _role), rows in sorted(grouped.items()):
            for row in rows:
                nodes = sorted(placements.get(row.get("session", ""), set()))
                writer.writerow({
                    "placement_pair": " + ".join(nodes) if len(nodes) == 2 else "incomplete",
                    "deployment": deployment,
                    **{field: row.get(field, "") for field in timeline_fields},
                })
    print(f"session_timeline_csv={timeline_path}")

    print("metrics_us_or_ms_by_node_role:")
    for (deployment, role), rows in sorted(grouped.items()):
        print(f"  {deployment} role={role} samples={len(rows)}")
        for metric in METRICS:
            values = [float(row[metric]) for row in rows if row.get(metric, "").isdigit()]
            if not values:
                continue
            unit = "ms" if metric == "total_ms" else "us"
            print(
                f"    {metric}: p50={percentile(values, 0.50):.0f}{unit} "
                f"p95={percentile(values, 0.95):.0f}{unit} "
                f"p99={percentile(values, 0.99):.0f}{unit}"
            )
    print(f"raw_evidence_dir={evidence_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
