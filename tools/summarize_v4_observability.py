#!/usr/bin/env python3
"""Aggregate V4 benchmark control-plane and Chunk metrics from structured logs."""

import argparse
import glob
import json
import math
import re
from datetime import datetime
from pathlib import Path

FIELD_RE = re.compile(r"(?:^|\s)([A-Za-z_]+)=([^\s]+)")
CONTROL_EVENTS = {"upload_session_create", "upload_session_get", "route_plan",
                  "chunk_commit", "file_commit", "object_manifest", "file_manifest"}
CHUNK_FIELDS = ("total_ms", "body_receive_ms", "replica_ms", "gateway_commit_ms",
                "sha_update_us", "pwrite_us", "sha_finalize_us", "index_us")
MAX_FIELDS = ("max_pending_bytes", "disk_queue_peak_bytes", "disk_pause_count", "disk_pause_ms")


def percentile(values, fraction):
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, math.ceil(fraction * len(values)) - 1)]


def summary(values):
    return {"count": len(values), "min": min(values) if values else 0.0,
            "p50": percentile(values, .50), "p95": percentile(values, .95),
            "p99": percentile(values, .99), "max": max(values) if values else 0.0,
            "mean": sum(values) / len(values) if values else 0.0}


def parse(line):
    fields = dict(FIELD_RE.findall(line))
    if "event" not in fields:
        return None, None
    try:
        timestamp = datetime.fromisoformat(fields["ts"]).timestamp()
    except (KeyError, ValueError):
        timestamp = None
    return fields, timestamp


def numeric(fields, key):
    try:
        return float(fields[key])
    except (KeyError, ValueError):
        return None


def qps(timestamps):
    if len(timestamps) < 2:
        return 0.0
    elapsed = max(timestamps) - min(timestamps)
    return len(timestamps) / elapsed if elapsed > 0 else 0.0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--logs", nargs="+", required=True, help="log paths or glob patterns")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    paths = sorted({Path(item) for pattern in args.logs for item in glob.glob(pattern)})
    if not paths:
        parser.error("no log files matched --logs")

    control = {}
    chunk_values = {key: [] for key in CHUNK_FIELDS}
    maxima = {key: 0.0 for key in MAX_FIELDS}
    snapshots = []
    event_loop_snapshots = {}
    chunk_timestamps, chunk_count, failed_chunks = [], 0, 0
    for path in paths:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            fields, timestamp = parse(line)
            if fields is None:
                continue
            event = fields["event"]
            if event == "resource_snapshot":
                snapshot = {key: numeric(fields, key) for key in (
                    "active_uploads", "active_downloads", "disk_queued_tasks",
                    "disk_queue_peak_tasks", "block_available", "block_total",
                    "block_leased_bytes", "block_peak_leased_bytes",
                    "disk_active_workers", "disk_total_workers", "disk_completed_tasks",
                    "output_buffer_current_bytes", "output_buffer_peak_bytes", "output_buffer_high_water_events")}
                snapshots.append({key: value for key, value in snapshot.items() if value is not None})
            if event == "event_loop_snapshot":
                loop_key = fields.get("node", "unknown") + ":" + fields.get("loop_index", "unknown")
                item = {key: numeric(fields, key) for key in (
                    "loop_iterations", "pending_queued", "cross_thread_queued", "pending_executed",
                    "pending_depth", "pending_peak_depth", "timer_callbacks", "timer_late_callbacks",
                    "timer_lag_total_ms", "timer_lag_max_ms")}
                item["loop_role"] = fields.get("loop_role", "unknown")
                event_loop_snapshots.setdefault(loop_key, []).append(
                    {key: value for key, value in item.items() if value is not None})
            if event in CONTROL_EVENTS:
                item = control.setdefault(event, {"latencies": [], "timestamps": [], "outcomes": {}})
                latency = numeric(fields, "elapsed_us")
                if latency is not None:
                    item["latencies"].append(latency)
                if timestamp is not None:
                    item["timestamps"].append(timestamp)
                outcome = fields.get("status", fields.get("found", fields.get("created", "ok")))
                item["outcomes"][outcome] = item["outcomes"].get(outcome, 0) + 1
            if event not in {"chunk_complete", "chunk_failed"}:
                continue
            # Secondary nodes emit the same event for their local replica
            # write. Client-visible Chunk latency is defined by the primary.
            if fields.get("role") != "primary":
                continue
            chunk_count += 1
            failed_chunks += event == "chunk_failed"
            if timestamp is not None:
                chunk_timestamps.append(timestamp)
            for key in CHUNK_FIELDS:
                value = numeric(fields, key)
                if value is not None:
                    chunk_values[key].append(value)
            for key in MAX_FIELDS:
                value = numeric(fields, key)
                if value is not None:
                    maxima[key] = max(maxima[key], value)

    result = {
        "input_logs": [str(path) for path in paths],
        "control_plane": {event: {"latency_us": summary(item["latencies"]),
                                   "qps_observed_window": qps(item["timestamps"]),
                                   "outcomes": item["outcomes"]}
                          for event, item in control.items()},
        "chunks": {"count": chunk_count, "failed": failed_chunks,
                   "qps_observed_window": qps(chunk_timestamps),
                   "latency": {key: summary(values) for key, values in chunk_values.items()},
                   "maxima": maxima},
        "resource_snapshots": {
            "count": len(snapshots),
            "max_active_uploads": max((item.get("active_uploads", 0.0) for item in snapshots), default=0.0),
            "max_active_downloads": max((item.get("active_downloads", 0.0) for item in snapshots), default=0.0),
            "max_queued_tasks": max((item.get("disk_queued_tasks", 0.0) for item in snapshots), default=0.0),
            "max_queue_peak_tasks": max((item.get("disk_queue_peak_tasks", 0.0) for item in snapshots), default=0.0),
            "min_available_blocks": min((item.get("block_available", 0.0) for item in snapshots), default=0.0),
            "max_leased_bytes": max((item.get("block_leased_bytes", 0.0) for item in snapshots), default=0.0),
            "max_peak_leased_bytes": max((item.get("block_peak_leased_bytes", 0.0) for item in snapshots), default=0.0),
            "max_active_workers": max((item.get("disk_active_workers", 0.0) for item in snapshots), default=0.0),
            "max_completed_tasks": max((item.get("disk_completed_tasks", 0.0) for item in snapshots), default=0.0),
            "max_output_buffer_current_bytes": max((item.get("output_buffer_current_bytes", 0.0) for item in snapshots), default=0.0),
            "max_output_buffer_peak_bytes": max((item.get("output_buffer_peak_bytes", 0.0) for item in snapshots), default=0.0),
            "max_output_buffer_high_water_events": max((item.get("output_buffer_high_water_events", 0.0) for item in snapshots), default=0.0),
        },
        "event_loops": {
            key: {"role": values[-1].get("loop_role", "unknown"), "snapshots": len(values),
                  "max_timer_lag_ms": max((item.get("timer_lag_max_ms", 0.0) for item in values), default=0.0),
                  "max_timer_late_callbacks": max((item.get("timer_late_callbacks", 0.0) for item in values), default=0.0),
                  "max_cross_thread_queued": max((item.get("cross_thread_queued", 0.0) for item in values), default=0.0),
                  "max_pending_queued": max((item.get("pending_queued", 0.0) for item in values), default=0.0),
                  "max_pending_depth": max((item.get("pending_depth", 0.0) for item in values), default=0.0),
                  "max_pending_peak_depth": max((item.get("pending_peak_depth", 0.0) for item in values), default=0.0),
                  "max_loop_iterations": max((item.get("loop_iterations", 0.0) for item in values), default=0.0)}
            for key, values in event_loop_snapshots.items()
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
