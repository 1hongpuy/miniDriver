from __future__ import annotations

import mimetypes
import statistics
from pathlib import Path
from typing import Any


_RASTER_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}
_RAW_SUFFIXES = {".arw", ".cr2", ".cr3", ".dng", ".nef", ".orf", ".raf", ".rw2"}
_VIDEO_SUFFIXES = {".mp4", ".mov", ".mkv", ".avi", ".webm"}


def media_kind(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in _RASTER_SUFFIXES:
        return "raster_image"
    if suffix in _RAW_SUFFIXES:
        return "raw_image"
    if suffix in _VIDEO_SUFFIXES:
        return "video"
    guessed, _encoding = mimetypes.guess_type(path.name)
    if guessed and guessed.startswith("image/"):
        return "raster_image"
    return "other"


def analyze_media(path: Path) -> dict[str, Any]:
    """Return deterministic, explainable metadata without changing object bytes.

    The quality values are deliberately heuristic, not learned labels. They are
    useful for filtering and observability, but must not be represented as a
    trained aesthetic/quality model.
    """
    result: dict[str, Any] = {"media_kind": media_kind(path)}
    if not path.is_file() or result["media_kind"] != "raster_image":
        return result
    try:
        from PIL import Image, ImageFilter, ImageStat  # type: ignore

        with Image.open(path) as source:
            image = source.convert("RGB")
            thumbnail = image.copy()
            thumbnail.thumbnail((256, 256))
            grayscale = thumbnail.convert("L")
            brightness = ImageStat.Stat(grayscale).mean[0] / 255.0
            contrast = ImageStat.Stat(grayscale).stddev[0] / 64.0
            edges = grayscale.filter(ImageFilter.FIND_EDGES)
            edge_stddev = ImageStat.Stat(edges).stddev[0]
            sharpness = min(1.0, edge_stddev / 48.0)
            result.update({
                "quality_method": "pillow-heuristic-v1",
                "brightness": round(brightness, 4),
                "contrast": round(min(1.0, contrast), 4),
                "sharpness": round(sharpness, 4),
                "exposure": "underexposed" if brightness < 0.20 else
                            "overexposed" if brightness > 0.85 else "normal",
                "quality_score": round((min(1.0, contrast) + sharpness +
                                        (1.0 - min(1.0, abs(brightness - 0.5) * 2))) / 3.0, 4),
                "perceptual_hash": _average_hash(grayscale),
            })
    except Exception as exc:
        result["quality_warning"] = f"image analysis unavailable: {type(exc).__name__}"
    return result


def _average_hash(image: Any) -> str:
    sampled = image.resize((8, 8))
    values = list(sampled.getdata())
    threshold = statistics.fmean(values)
    bits = "".join("1" if value >= threshold else "0" for value in values)
    return f"{int(bits, 2):016x}"


def hash_distance(left: str, right: str) -> int:
    """Hamming distance for two 64-bit average hashes."""
    if len(left) != 16 or len(right) != 16:
        raise ValueError("perceptual hashes must be 16 hex characters")
    return (int(left, 16) ^ int(right, 16)).bit_count()
