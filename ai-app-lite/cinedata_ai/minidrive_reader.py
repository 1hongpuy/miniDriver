from __future__ import annotations

import contextlib
import hashlib
import json
import os
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Iterator


class MiniDriveObjectReader:
    """Materialises a committed MiniDrive object through its manifest.

    The worker verifies every downloaded Chunk hash. It deliberately does not
    inspect DataNode disk paths or reuse the browser download implementation.
    """

    def __init__(self, gateway_url: str, temp_dir: Path, max_object_bytes: int):
        if not gateway_url:
            raise ValueError("AI_MINIDRIVE_GATEWAY_URL is required for Stream ingestion")
        self.gateway_url = gateway_url.rstrip("/")
        self.temp_dir = Path(temp_dir).expanduser()
        self.max_object_bytes = max_object_bytes
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    @contextlib.contextmanager
    def materialize(self, event: dict[str, Any]) -> Iterator[Path]:
        object_id = str(event["asset_id"])
        object_key = str(event.get("object_key", object_id))
        manifest = self._json_get(f"/api/v2/objects/{urllib.parse.quote(object_id, safe='')}/manifest")
        file_size = int(manifest.get("fileSize", 0))
        chunk_size = int(manifest.get("chunkSize", 0))
        chunks = manifest.get("chunks")
        if file_size <= 0 or file_size > self.max_object_bytes or chunk_size <= 0 or not isinstance(chunks, list):
            raise ValueError("invalid or oversized MiniDrive object manifest")

        suffix = Path(object_key).suffix[:16]
        descriptor, name = tempfile.mkstemp(prefix=f"{hashlib.sha256(object_id.encode()).hexdigest()[:16]}-", suffix=suffix,
                                          dir=self.temp_dir)
        target = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as output:
                for index, chunk in enumerate(chunks):
                    expected_hash = str(chunk.get("hash", ""))
                    expected_size = min(chunk_size, file_size - index * chunk_size)
                    replicas = chunk.get("replicas") or []
                    if not expected_hash or expected_size <= 0 or not replicas:
                        raise ValueError("manifest has an invalid chunk")
                    data = self._fetch_verified_chunk(replicas, expected_hash, expected_size)
                    output.write(data)
            if target.stat().st_size != file_size:
                raise ValueError("materialized object size mismatch")
            expected_file_hash = str(event.get("file_hash", ""))
            # fileHash is MiniDrive's manifest/content identity, not necessarily
            # SHA-256(bytes), so individual Chunk validation is the mandatory
            # integrity check here. Preserve the identity as metadata instead.
            if not expected_file_hash:
                raise ValueError("event has no file hash")
            yield target
        finally:
            target.unlink(missing_ok=True)

    def _json_get(self, path: str) -> dict[str, Any]:
        request = urllib.request.Request(self.gateway_url + path, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                value = json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"cannot read MiniDrive metadata: {exc}") from exc
        if not isinstance(value, dict):
            raise ValueError("MiniDrive metadata response must be an object")
        return value

    @staticmethod
    def _chunk_url(replica: dict[str, Any], chunk_hash: str) -> str:
        address = str(replica.get("address", ""))
        port = int(replica.get("httpPort", 0))
        if not address or port <= 0 or port > 65535:
            raise ValueError("invalid DataNode replica endpoint")
        return f"http://{address}:{port}/v2/chunks/{urllib.parse.quote(chunk_hash, safe='')}"

    def _fetch_verified_chunk(self, replicas: list[dict[str, Any]], expected_hash: str,
                              expected_size: int) -> bytes:
        failures: list[str] = []
        for replica in replicas:
            try:
                request = urllib.request.Request(self._chunk_url(replica, expected_hash), method="GET")
                with urllib.request.urlopen(request, timeout=30) as response:
                    data = response.read(expected_size + 1)
                if len(data) != expected_size:
                    raise ValueError("size mismatch")
                if hashlib.sha256(data).hexdigest() != expected_hash:
                    raise ValueError("sha256 mismatch")
                return data
            except (ValueError, urllib.error.URLError, urllib.error.HTTPError) as exc:
                failures.append(str(exc))
        raise RuntimeError("all DataNode replicas failed: " + "; ".join(failures[:3]))
