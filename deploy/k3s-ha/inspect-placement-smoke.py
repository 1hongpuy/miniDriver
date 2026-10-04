#!/usr/bin/env python3
"""Summarize a persisted K3s placement-smoke benchmark report directory."""

import argparse
import csv
import json
from collections import Counter
from pathlib import Path


def is_success(row: dict[str, str]) -> bool:
    return (
        row.get("upload_ok") == "true"
        and row.get("download_ok") == "true"
        and not row.get("error", "")
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "report_dir",
        nargs="?",
        type=Path,
        default=Path("/var/lib/minidriver-bench/placement-smoke"),
    )
    parser.add_argument("--failed-samples", type=int, default=12)
    args = parser.parse_args()

    report_dir = args.report_dir
    runs_path = report_dir / "runs.csv"
    summary_path = report_dir / "summary.csv"
    stages_path = report_dir / "upload-stages-summary.csv"
    json_path = report_dir / "summary.json"
    for path in (runs_path, summary_path, stages_path, json_path):
        if not path.is_file():
            raise SystemExit(f"missing report: {path}")

    with runs_path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))

    successes = [row for row in rows if is_success(row)]
    failures = [row for row in rows if not is_success(row)]
    errors = Counter(
        row.get("error") or "missing success flag without an error message"
        for row in failures
    )

    print(f"report_dir={report_dir}")
    print(f"attempts={len(rows)} success={len(successes)} failed={len(failures)}")
    if successes:
        latest = successes[-1]
        print(
            "latest_success_object="
            f"{latest.get('object_id', '')} "
            f"version={latest.get('object_version', '')} "
            f"session={latest.get('upload_session_id', '')}"
        )
    print("error_counts:")
    for error, count in errors.most_common():
        print(f"  {count:>4}  {error}")

    with summary_path.open(newline="", encoding="utf-8") as stream:
        summaries = list(csv.DictReader(stream))
    print("summary:")
    for summary in summaries:
        print(
            "  size={size_bytes} attempts={upload_attempt_count} "
            "success={upload_success_count} p50={upload_median_ms}ms "
            "p95={upload_p95_ms}ms p99={upload_p99_ms}ms".format(**summary)
        )

    with stages_path.open(newline="", encoding="utf-8") as stream:
        stages = list(csv.DictReader(stream))
    print("upload_stages:")
    for stage in stages:
        print(
            "  {stage}: samples={samples} p50={p50_ms}ms "
            "p95={p95_ms}ms p99={p99_ms}ms".format(**stage)
        )

    with json_path.open(encoding="utf-8") as stream:
        summary_json = json.load(stream)
    print(f"summary_json_keys={','.join(sorted(summary_json.keys()))}")

    if failures:
        print("failed_samples:")
        for row in failures[: args.failed_samples]:
            print(
                "  run={run_id} upload_ok={upload_ok} download_ok={download_ok} "
                "create={upload_create_session_ms}ms route={upload_route_capability_ms}ms "
                "data={upload_data_node_ms}ms commit={upload_object_commit_ms}ms "
                "error={error}".format(**row)
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
