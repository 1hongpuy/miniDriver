from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from .store import AssetStore


_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff"}


def enqueue_labeled_directory(store: AssetStore, root: Path) -> tuple[int, list[dict[str, Any]]]:
    """Enqueue a folder-labelled local evaluation corpus deterministically.

    This helper is intentionally local-only: it is for model quality evaluation
    before the same photos are uploaded through the MiniDrive event path.
    """
    root = Path(root).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"dataset root is not a directory: {root}")
    cases: dict[str, list[str]] = {}
    queued = 0
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in _IMAGE_SUFFIXES:
            continue
        relative = path.relative_to(root)
        if len(relative.parts) < 2:
            raise ValueError(f"dataset file must be inside a label directory: {path}")
        label = relative.parts[0]
        asset_id = "local-" + hashlib.sha256(str(relative).encode("utf-8")).hexdigest()[:24]
        store.enqueue_asset({
            "asset_id": asset_id,
            "object_key": "/" + relative.as_posix(),
            "local_path": str(path),
            "metadata": {
                "dataset_name": root.name,
                "ground_truth_label": label,
                "ground_truth_source": "parent_directory",
            },
        })
        cases.setdefault(label, []).append(asset_id)
        queued += 1
    if not queued:
        raise ValueError("dataset has no supported raster images")
    return queued, [
        {"query": label, "expected_asset_ids": asset_ids}
        for label, asset_ids in sorted(cases.items())
        if label != "其他干扰项"
    ]
