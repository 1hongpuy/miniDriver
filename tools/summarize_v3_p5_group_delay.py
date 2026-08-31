#!/usr/bin/env python3
"""Print a compact comparison table for the P5 RF=2 Group Commit delay scan."""

import argparse
import glob
import json
import re
from pathlib import Path


FIELD_RE = re.compile(r"(?:^|\s)([A-Za-z_]+)=([^\s]+)")
THROUGHPUT_RE = re.compile(r"round aggregate=([0-9.]+) MiB/s")


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def batch_stats(case_root):
    batches = {}
    chunks = 0
    for name in glob.glob(str(case_root / "cluster/node-*/logs/datanode-*.log")):
        path = Path(name)
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            fields = dict(FIELD_RE.findall(line))
            if fields.get("event") != "chunk_complete":
                continue
            chunks += 1
            sequence = fields.get("durable_sequence")
            if sequence is None:
                continue
            key = (str(path), sequence)
            batches[key] = (int(fields.get("group_batch_items", "0")),
                            int(fields.get("group_batch_bytes", "0")))
    items = [value[0] for value in batches.values()]
    full = sum(value[1] >= 8 * 1024 * 1024 for value in batches.values())
    return {
        "chunks_all_nodes": chunks,
        "batches_all_nodes": len(items),
        "batch_items_mean": sum(items) / len(items) if items else 0.0,
        "full_batch_percent": 100.0 * full / len(items) if items else 0.0,
    }


def latency(observability, key, percentile):
    return observability["chunks"]["latency"][key][percentile]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()

    rows = []
    for workload in ("sustained-c4", "mixed-c8"):
        for delay_ms in (2, 3, 4, 5):
            case_root = args.root / workload / f"{delay_ms}ms"
            result_root = case_root / "results"
            client = load_json(result_root / "summary.json")
            summary = client["summaries"][0]
            observability = load_json(result_root / "observability.json")
            throughputs = [float(value) for value in THROUGHPUT_RE.findall(
                (result_root / "console.log").read_text(encoding="utf-8"))]
            batches = batch_stats(case_root)
            rows.append({
                "workload": workload,
                "delay_ms": delay_ms,
                "throughput_mean": sum(throughputs) / len(throughputs),
                "success": summary["successCount"],
                "requested": client["uploadSamplesRequested"] + client["downloadSamplesRequested"],
                "upload_p95_ms": summary["uploadP95Ms"],
                "download_p95_ms": summary["downloadP95Ms"],
                "group_queue_p50_ms": latency(observability, "group_queue_wait_us", "p50") / 1000.0,
                "group_queue_p95_ms": latency(observability, "group_queue_wait_us", "p95") / 1000.0,
                "group_commit_p95_ms": latency(observability, "group_commit_us", "p95") / 1000.0,
                "group_wait_p95_ms": latency(observability, "group_wait_us", "p95") / 1000.0,
                **batches,
            })

    print("workload delay throughput success upload_p95 download_p95 queue_p50 queue_p95 "
          "commit_p95 total_wait_p95 avg_batch_items full_batch")
    for row in rows:
        print(f"{row['workload']:12s} {row['delay_ms']}ms "
              f"{row['throughput_mean']:8.2f} {row['success']}/{row['requested']} "
              f"{row['upload_p95_ms']:8.1f} {row['download_p95_ms']:8.1f} "
              f"{row['group_queue_p50_ms']:8.1f} {row['group_queue_p95_ms']:8.1f} "
              f"{row['group_commit_p95_ms']:8.1f} {row['group_wait_p95_ms']:8.1f} "
              f"{row['batch_items_mean']:6.2f} {row['full_batch_percent']:6.1f}%")


if __name__ == "__main__":
    main()
