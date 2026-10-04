#!/usr/bin/env python3
"""Classify benchmark failures without modifying the original runs.csv."""
from __future__ import annotations

import argparse
import csv
import pathlib
import sys


def classify(error: str) -> str:
    text = (error or "").lower()
    if not text:
        return "success"
    if "timeout" in text or "timed out" in text or "deadline" in text:
        return "timeout"
    if "503" in text or "admission" in text or "capacity" in text or "write capacity" in text:
        return "503/admission"
    if "route" in text and ("retry" in text or "reconnect" in text or "failed" in text):
        return "route retry"
    if "replica" in text:
        return "replica failure"
    if "lease" in text or "version" in text or "conflict" in text:
        return "lease/version conflict"
    if "connection reset" in text or "reset by peer" in text or "socket" in text:
        return "connection reset"
    if any(token in text for token in ("crc", "sha-256", "checksum", "objectref", "object ref")):
        return "CRC/ObjectRef validation"
    return "other"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=pathlib.Path)
    parser.add_argument("-o", "--output", type=pathlib.Path, required=True)
    args = parser.parse_args()

    fields = [
        "source_runs_csv", "run_id", "operation", "upload_session_id",
        "object_id", "object_version", "error", "category",
        "start_offset_ms", "end_offset_ms",
    ]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=fields)
        writer.writeheader()
        for source in args.inputs:
            if not source.is_file():
                print(f"missing runs.csv: {source}", file=sys.stderr)
                continue
            with source.open(newline="", encoding="utf-8") as stream:
                for row in csv.DictReader(stream):
                    error = row.get("error", "")
                    if not error:
                        continue
                    writer.writerow({
                        "source_runs_csv": str(source),
                        "run_id": row.get("run_id", ""),
                        "operation": row.get("operation", ""),
                        "upload_session_id": row.get("upload_session_id", ""),
                        "object_id": row.get("object_id", ""),
                        "object_version": row.get("object_version", ""),
                        "error": error,
                        "category": classify(error),
                        "start_offset_ms": row.get("start_offset_ms", ""),
                        "end_offset_ms": row.get("end_offset_ms", ""),
                    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
