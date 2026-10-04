#!/usr/bin/env python3
"""Summarize CSV-like output emitted by minikv_storage_sync_probe."""

import argparse
import math
import re
from collections import defaultdict
from pathlib import Path


def tokens(line: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in line.strip().split(",")[1:]:
        if "=" in item:
            key, value = item.split("=", 1)
            result[key] = value
    return result


def percentile(values: list[int], fraction: float) -> int:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)] if ordered else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("report_dir", type=Path)
    args = parser.parse_args()
    groups: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for path in sorted(args.report_dir.glob("*.csv")):
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.startswith("sample,"):
                continue
            row = tokens(line)
            node = re.search(r"-n([0-9]+)-", path.stem)
            mode = "concurrent" if path.stem.startswith("concurrent-") else "single"
            groups[(mode, row.get("label", "?"), row.get("size_bytes", "?"), node.group(1) if node else "?")].append(row)
    print("mode,label,node,size_bytes,samples,pwrite_p50_us,pwrite_p95_us,pwrite_p99_us,fdatasync_p50_us,fdatasync_p95_us,fdatasync_p99_us,total_p50_us,total_p95_us,total_p99_us,overlap_start_ms,overlap_end_ms")
    for (mode, label, size, node), rows in sorted(groups.items()):
        values = lambda key: [int(row[key]) for row in rows if row.get(key, "").isdigit()]
        starts = values("start_unix_ms")
        ends = values("end_unix_ms")
        print(
            f"{mode},{label},{node},{size},{len(rows)},"
            f"{percentile(values('pwrite_us'), .50)},{percentile(values('pwrite_us'), .95)},{percentile(values('pwrite_us'), .99)},"
            f"{percentile(values('fdatasync_us'), .50)},{percentile(values('fdatasync_us'), .95)},{percentile(values('fdatasync_us'), .99)},"
            f"{percentile(values('total_us'), .50)},{percentile(values('total_us'), .95)},{percentile(values('total_us'), .99)},"
            f"{min(starts) if starts else 0},{max(ends) if ends else 0}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
