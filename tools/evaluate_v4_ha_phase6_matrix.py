#!/usr/bin/env python3
"""Evaluate the MiniDriver 4.0-HA Phase 6 release gates.

The benchmark runner deliberately records short and failed runs instead of
turning them into a green result.  This evaluator is a separate, deterministic
step: it compares each Raft/HA case with the matching legacy case and writes a
machine-readable gate report.  It does not change the default mode or mutate
benchmark artifacts.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Dict, Iterable, Tuple


def read_manifest(path: Path) -> Iterable[dict]:
    with path.open(newline="", encoding="utf-8") as stream:
        yield from csv.DictReader(stream, delimiter="\t")


def read_summary(work_dir: Path) -> dict:
    summary = work_dir / "summary.json"
    if summary.exists():
        with summary.open(encoding="utf-8") as stream:
            payload = json.load(stream)
        # The benchmark JSON wraps the per-size result in `summaries`.
        # The evaluator compares one manifest row (one size/concurrency/
        # operation), so normalize that wrapper here.
        summaries = payload.get("summaries") if isinstance(payload, dict) else None
        if isinstance(summaries, list) and summaries:
            return summaries[0]
        return payload
    csv_path = work_dir / "summary.csv"
    with csv_path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError(f"empty summary: {csv_path}")
    return rows[0]


def numeric(summary: dict, key: str) -> float:
    aliases = {
        "upload_success_count": ("upload_success_count", "uploadSuccessCount"),
        "download_success_count": ("download_success_count", "downloadSuccessCount"),
        "upload_attempt_count": ("upload_attempt_count", "uploadAttemptCount"),
        "download_attempt_count": ("download_attempt_count", "downloadAttemptCount"),
        "upload_mib_per_sec": ("upload_mib_per_sec", "uploadMiBPerSecond"),
        "download_mib_per_sec": ("download_mib_per_sec", "downloadMiBPerSecond"),
        "upload_p99_ms": ("upload_p99_ms", "uploadP99Ms"),
        "download_p99_ms": ("download_p99_ms", "downloadP99Ms"),
    }
    value = 0
    for alias in aliases.get(key, (key,)):
        if alias in summary:
            value = summary[alias]
            break
    if isinstance(value, (int, float)):
        return float(value)
    return float(value or 0)


def summary_for(row: dict, root: Path) -> dict:
    return read_summary(Path(row["work_dir"]))


def key(row: dict) -> Tuple[str, str, str, str]:
    return row["repeat"], row["size"], row["concurrency"], row["operation"]


def csv_values(raw: str) -> Tuple[str, ...]:
    """Parse a comma-separated CLI list while rejecting empty entries."""
    values = tuple(item.strip() for item in raw.split(","))
    if not values or any(not item for item in values):
        raise ValueError(f"invalid comma-separated value: {raw!r}")
    return values


def validate_release_matrix(rows: Iterable[dict], args: argparse.Namespace) -> Tuple[bool, str]:
    """Require the exact release dimensions before applying throughput gates.

    Short diagnostic runs intentionally remain supported by the default mode.
    This explicit check prevents a partial manifest from being mistaken for a
    passing release matrix when the caller opts into release qualification.
    """
    rows = list(rows)
    try:
        expected_topologies = csv_values(args.release_topologies)
        expected_sizes = csv_values(args.release_sizes)
        expected_concurrencies = csv_values(args.release_concurrencies)
    except ValueError as exc:
        return False, str(exc)

    expected_repeats = tuple(str(index) for index in range(1, args.release_repeats + 1))
    expected_keys = {
        (topology, repeat, size, concurrency, operation)
        for topology in expected_topologies
        for repeat in expected_repeats
        for size in expected_sizes
        for concurrency in expected_concurrencies
        for operation in ("upload", "download")
    }

    seen = {}
    for row in rows:
        row_key = (row.get("topology", ""),) + key(row)
        seen[row_key] = seen.get(row_key, 0) + 1

    missing = sorted(expected_keys - set(seen))
    duplicates = sorted(item for item, count in seen.items() if count > 1 and item in expected_keys)
    invalid_status = sorted(
        (item, row.get("status", ""))
        for row in rows
        for item in [(row.get("topology", ""),) + key(row)]
        if item in expected_keys and row.get("status") != "passed"
    )
    bad_dimensions = []
    for row in rows:
        if row.get("topology") not in expected_topologies:
            continue
        if row.get("repeat") not in expected_repeats:
            continue
        if row.get("size") not in expected_sizes:
            continue
        if row.get("concurrency") not in expected_concurrencies:
            continue
        try:
            warmup = int(row.get("warmup_s", ""))
            steady = int(row.get("steady_s", ""))
        except ValueError:
            bad_dimensions.append((row.get("topology"), key(row), "non-numeric duration"))
            continue
        if warmup != args.release_warmup or steady != args.release_steady:
            bad_dimensions.append((row.get("topology"), key(row), f"duration={warmup}/{steady}"))

    problems = []
    if missing:
        problems.append(f"missing={len(missing)} (first={missing[:3]})")
    if duplicates:
        problems.append(f"duplicate={len(duplicates)} (first={duplicates[:3]})")
    if invalid_status:
        problems.append(f"not_passed={len(invalid_status)} (first={invalid_status[:3]})")
    if bad_dimensions:
        problems.append(f"wrong_dimensions={len(bad_dimensions)} (first={bad_dimensions[:3]})")
    if problems:
        return False, "release matrix incomplete: " + "; ".join(problems)
    return True, "release matrix dimensions complete"


def p99(summary: dict, operation: str) -> float:
    return numeric(summary, "upload_p99_ms" if operation == "upload" else "download_p99_ms")


def independent_wal_check(matrix: Path) -> Tuple[bool, str]:
    """Verify that the recorded WAL root is persistent and on another device.

    The benchmark can still be evaluated without this check for short
    diagnostics.  Release qualification must opt in explicitly because a
    directory path (or tmpfs) is not evidence of a separate durable device.
    """
    report = matrix / "storage.tsv"
    if not report.exists():
        return False, "storage.tsv is missing"
    rows = []
    with report.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream, delimiter="\t"))
    if len(rows) < 2:
        return False, "WAL storage identity is missing"
    workspace = rows[0]
    candidates = rows[1:]
    for wal in candidates:
        fstype = wal.get("fstype", "").lower()
        if fstype in {"tmpfs", "ramfs", "devtmpfs"}:
            continue
        if wal.get("device_id") and wal.get("device_id") != workspace.get("device_id"):
            return True, "WAL is on a different non-temporary device"
    return False, "WAL is missing, temporary, or on the same device as the benchmark workspace"


def throughput(summary: dict, operation: str) -> float:
    return numeric(summary, "upload_mib_per_sec" if operation == "upload" else "download_mib_per_sec")


def attempts(summary: dict, operation: str) -> Tuple[float, float]:
    if operation == "upload":
        return numeric(summary, "upload_success_count"), numeric(summary, "upload_attempt_count")
    return numeric(summary, "download_success_count"), numeric(summary, "download_attempt_count")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("matrix", type=Path, help="Phase 6 output directory containing manifest.tsv")
    parser.add_argument("--output", type=Path, help="gate report path (default: matrix/gate-report.tsv)")
    parser.add_argument(
        "--require-independent-wal",
        action="store_true",
        help="require storage.tsv to prove a non-tmpfs WAL on a different device",
    )
    parser.add_argument(
        "--require-release-matrix",
        action="store_true",
        help="require the complete 30s/300s x 3-topology x 2-size x 4-concurrency x 3-repeat matrix",
    )
    parser.add_argument(
        "--release-topologies",
        default="legacy,raft,ha",
        help="topologies required by --require-release-matrix",
    )
    parser.add_argument(
        "--release-sizes",
        default="64KiB,16MiB",
        help="object sizes required by --require-release-matrix",
    )
    parser.add_argument(
        "--release-concurrencies",
        default="1,4,8,16",
        help="concurrency values required by --require-release-matrix",
    )
    parser.add_argument(
        "--release-repeats",
        type=int,
        default=3,
        help="number of repeats required by --require-release-matrix",
    )
    parser.add_argument(
        "--release-warmup",
        type=int,
        default=30,
        help="warmup seconds required by --require-release-matrix",
    )
    parser.add_argument(
        "--release-steady",
        type=int,
        default=300,
        help="steady seconds required by --require-release-matrix",
    )
    args = parser.parse_args()
    if args.release_repeats < 1 or args.release_warmup < 0 or args.release_steady <= args.release_warmup:
        parser.error("invalid release matrix dimensions")
    manifest_path = args.matrix / "manifest.tsv"
    if not manifest_path.exists():
        parser.error(f"missing {manifest_path}")

    rows = list(read_manifest(manifest_path))
    report_path = args.output or (args.matrix / "gate-report.tsv")
    indexed: Dict[Tuple[str, str, str, str], dict] = {}
    summaries: Dict[Tuple[str, str, str, str], dict] = {}
    storage_ok = True
    storage_reason = "not requested"
    if args.require_independent_wal:
        storage_ok, storage_reason = independent_wal_check(args.matrix)
    release_matrix_ok = True
    release_matrix_reason = "not requested"
    if args.require_release_matrix:
        release_matrix_ok, release_matrix_reason = validate_release_matrix(rows, args)
    for row in rows:
        if row.get("status") == "SKIP":
            continue
        item_key = key(row)
        indexed[(row["topology"],) + item_key] = row
        work = Path(row["work_dir"])
        if work.exists() and ((work / "summary.json").exists() or (work / "summary.csv").exists()):
            try:
                summaries[(row["topology"],) + item_key] = summary_for(row, args.matrix)
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                summaries[(row["topology"],) + item_key] = {"_error": str(exc)}

    fields = [
        "topology", "repeat", "size", "concurrency", "operation",
        "status", "legacy_throughput_mib_s", "candidate_throughput_mib_s",
        "throughput_ratio", "legacy_p99_ms", "candidate_p99_ms", "p99_ratio",
        "gate", "reason",
    ]
    output_rows = []
    overall = True
    if args.require_independent_wal and not storage_ok:
        overall = False
    if args.require_release_matrix and not release_matrix_ok:
        overall = False
    for candidate_topology in ("raft", "ha"):
        for item_key in sorted({k[1:] for k in indexed if k[0] == candidate_topology}):
            candidate_key = (candidate_topology,) + item_key
            legacy_key = ("legacy",) + item_key
            candidate_row = indexed[candidate_key]
            legacy_row = indexed.get(legacy_key)
            candidate = summaries.get(candidate_key, {})
            baseline = summaries.get(legacy_key, {})
            reason = ""
            gate = "PASS"
            if args.require_independent_wal and not storage_ok:
                gate, reason = "FAIL", storage_reason
            elif not legacy_row or not baseline:
                gate, reason = "FAIL", "matching legacy baseline is missing"
            elif candidate_row.get("status") != "passed":
                gate, reason = "FAIL", "candidate benchmark case failed"
            elif candidate.get("_error") or baseline.get("_error"):
                gate, reason = "FAIL", "summary could not be parsed"
            else:
                candidate_success, candidate_attempts = attempts(candidate, item_key[3])
                legacy_success, legacy_attempts = attempts(baseline, item_key[3])
                if candidate_attempts <= 0 or candidate_success != candidate_attempts:
                    gate, reason = "FAIL", "candidate has failed attempts"
                elif legacy_attempts <= 0 or legacy_success != legacy_attempts:
                    gate, reason = "FAIL", "legacy baseline has failed attempts"

            base_tp = throughput(baseline, item_key[3]) if baseline else 0.0
            candidate_tp = throughput(candidate, item_key[3]) if candidate else 0.0
            base_p99 = p99(baseline, item_key[3]) if baseline else 0.0
            candidate_p99 = p99(candidate, item_key[3]) if candidate else 0.0
            tp_ratio = candidate_tp / base_tp if base_tp > 0 else math.nan
            p99_ratio = candidate_p99 / base_p99 if base_p99 > 0 else math.nan

            # The plan's explicit gates apply to PUT.  For GET we still emit
            # the comparison, but do not invent a separate release threshold.
            if gate == "PASS" and item_key[3] == "upload":
                if item_key[1] == "64KiB":
                    if not math.isfinite(tp_ratio) or tp_ratio < 0.75:
                        gate, reason = "FAIL", "64 KiB PUT throughput is below 75% of legacy"
                    elif not math.isfinite(p99_ratio) or p99_ratio > 2.0:
                        gate, reason = "FAIL", "64 KiB PUT P99 exceeds 2x legacy"
                elif item_key[1] == "16MiB":
                    if not math.isfinite(tp_ratio) or tp_ratio < 0.90:
                        gate, reason = "FAIL", "16 MiB PUT throughput is below 90% of legacy"

            if gate != "PASS":
                overall = False
            output_rows.append({
                "topology": candidate_topology,
                "repeat": item_key[0], "size": item_key[1],
                "concurrency": item_key[2], "operation": item_key[3],
                "status": candidate_row.get("status", "missing"),
                "legacy_throughput_mib_s": f"{base_tp:.6f}",
                "candidate_throughput_mib_s": f"{candidate_tp:.6f}",
                "throughput_ratio": "nan" if not math.isfinite(tp_ratio) else f"{tp_ratio:.6f}",
                "legacy_p99_ms": f"{base_p99:.6f}",
                "candidate_p99_ms": f"{candidate_p99:.6f}",
                "p99_ratio": "nan" if not math.isfinite(p99_ratio) else f"{p99_ratio:.6f}",
                "gate": gate, "reason": reason,
            })

    with report_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, delimiter="\t")
        writer.writeheader()
        writer.writerows(output_rows)
    print(f"gate_report={report_path}")
    if args.require_release_matrix:
        print(f"release_matrix={'PASS' if release_matrix_ok else 'FAIL'}: {release_matrix_reason}")
    if args.require_independent_wal:
        print(f"independent_wal={'PASS' if storage_ok else 'FAIL'}: {storage_reason}")
    print(f"gate_status={'PASS' if overall and output_rows else 'FAIL'}")
    return 0 if overall and output_rows else 1


if __name__ == "__main__":
    raise SystemExit(main())
