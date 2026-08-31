from __future__ import annotations

from pathlib import Path
from typing import Any


def extract_metadata(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {"file_name": path.name, "suffix": path.suffix.lower()}
    if not path.is_file():
        return result
    result["size_bytes"] = path.stat().st_size
    try:
        from PIL import Image, ExifTags  # type: ignore

        with Image.open(path) as image:
            result["width"], result["height"] = image.size
            result["format"] = image.format
            exif = image.getexif()
            tags = {ExifTags.TAGS.get(key, str(key)): value for key, value in exif.items()}
            for source, target in {
                "DateTimeOriginal": "capture_time",
                "Make": "camera_make",
                "Model": "camera_model",
                "LensModel": "lens",
                "FocalLength": "focal_length",
            }.items():
                if source in tags:
                    result[target] = str(tags[source])
    except Exception:
        # Metadata extraction is best effort; indexing must still be able to continue.
        result["image_metadata_warning"] = "Pillow unavailable or file is not a readable image"
    return result
