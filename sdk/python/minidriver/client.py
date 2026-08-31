"""Object-level MiniDriver HTTP client.

Only ObjectRef is durable. Read plans and DataNode read tokens are obtained for
each actual read and are never exposed as a persistence contract.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import ProxyHandler, Request, build_opener


class MiniDriverError(RuntimeError):
    """A protocol, authorization, integrity, or transport error."""


@dataclass(frozen=True)
class ObjectRef:
    object_id: str
    object_version: int


@dataclass(frozen=True)
class ClientConfig:
    gateway_url: str
    cluster_internal_token: str
    service_principal: str
    gateway_timeout_seconds: float = 30.0
    datanode_timeout_seconds: float = 60.0


@dataclass(frozen=True)
class PutOptions:
    checksum_type: str = "crc32c"
    chunk_window: int = 2


@dataclass(frozen=True)
class ReadOptions:
    disable_checksum: bool = False
    allow_replica_retry: bool = True
    max_replica_attempts: int = 2


class IntegrityStatus(str, Enum):
    VERIFIED_WHOLE_CHUNK = "verified-whole-chunk"
    UNVERIFIED_PARTIAL_RANGE = "unverified-partial-range"
    UNVERIFIED_CHECKSUM_DISABLED = "unverified-checksum-disabled"


@dataclass
class TransferStats:
    data_requests: int = 0
    replica_fallbacks: int = 0
    bytes_verified: int = 0


@dataclass(frozen=True)
class RangeReadResult:
    bytes_read: int
    integrity: IntegrityStatus


@dataclass(frozen=True)
class ObjectInfo:
    object: ObjectRef
    metadata_version: int
    size: int
    name: str
    parent_path: str
    state: str


@dataclass(frozen=True)
class NodeReadHint:
    node_id: str
    local_bytes: int
    coverage_ratio: float
    health: str


@dataclass(frozen=True)
class ObjectReadHints:
    object: ObjectRef
    size: int
    candidates: tuple[NodeReadHint, ...]


@dataclass(frozen=True)
class _Replica:
    node_id: str
    address: str
    http_port: int


@dataclass(frozen=True)
class _ChunkPlan:
    index: int
    storage_identity: str
    size: int
    checksum_type: str
    checksum_digest: str
    read_capability: str
    replicas: tuple[_Replica, ...]


@dataclass(frozen=True)
class _ReadPlan:
    object: ObjectRef
    file_size: int
    chunk_size: int
    chunks: tuple[_ChunkPlan, ...]


class Client:
    """Thread-safe object API facade for Python services.

    urllib does not supply a shared keep-alive connection pool. Concurrent
    calls remain safe because this client has no mutable request state.
    """

    def __init__(self, config: ClientConfig) -> None:
        parsed = urlparse(config.gateway_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("gateway_url must be an absolute http(s) URL")
        if not config.cluster_internal_token or not config.service_principal:
            raise ValueError("cluster_internal_token and service_principal are required")
        self._config = config
        self._gateway = config.gateway_url.rstrip("/")
        self._gateway_parts = parsed
        # Gateway/DataNode are cluster-internal endpoints. Do not let ambient
        # HTTP_PROXY settings route capabilities or chunk bodies externally.
        self._opener = build_opener(ProxyHandler({}))

    def put_file(self, path: str | Path, options: PutOptions = PutOptions()) -> ObjectRef:
        source = Path(path)
        size = source.stat().st_size
        if size <= 0:
            raise MiniDriverError("upload source must be non-empty")
        if options.checksum_type not in ("crc32c", "sha256") or options.chunk_window <= 0:
            raise ValueError("checksum_type must be crc32c|sha256 and chunk_window must be positive")

        session = self._create_session(size)
        session_id = self._required_str(session, "sessionId")
        session = self._get_json("GET", f"/api/v2/upload/sessions/{_escape(session_id)}", control=False)
        chunk_size = self._required_int(session, "chunkSize")
        total_chunks = self._required_int(session, "totalChunks")
        checksum_type = self._required_str(session, "checksumType")
        if checksum_type != options.checksum_type:
            raise MiniDriverError(f"Gateway checksum policy {checksum_type} differs from requested {options.checksum_type}")
        completed = {int(index) for index in session.get("completed", [])}
        pending = [index for index in range(total_chunks) if index not in completed]
        with ThreadPoolExecutor(max_workers=min(options.chunk_window, len(pending) or 1)) as executor:
            futures = [executor.submit(self._put_chunk, source, size, session_id, chunk_size, index, checksum_type)
                       for index in pending]
            for future in as_completed(futures):
                future.result()
        committed = self._post_json(f"/api/v2/upload/sessions/{_escape(session_id)}/commit", {}, control=False)
        reference = ObjectRef(self._required_str(committed, "objectId"), self._required_int(committed, "objectVersion"))
        self._validate_ref(reference)
        return reference

    def get_object(self, reference: ObjectRef, sink: Callable[[bytes], None],
                   options: ReadOptions = ReadOptions()) -> TransferStats:
        if sink is None:
            raise ValueError("sink is required")
        plan = self._read_plan(reference)
        stats = TransferStats()
        delivered = 0
        for chunk in plan.chunks:
            body, _ = self._read_chunk(chunk, 0, chunk.size, options, stats)
            sink(body)
            delivered += len(body)
        if delivered != plan.file_size:
            raise MiniDriverError("object length mismatch")
        return stats

    def get_range(self, reference: ObjectRef, offset: int, length: int,
                  sink: Callable[[bytes], None], options: ReadOptions = ReadOptions()) -> tuple[RangeReadResult, TransferStats]:
        if sink is None or length <= 0:
            raise ValueError("sink and positive length are required")
        plan = self._read_plan(reference)
        if offset < 0 or offset >= plan.file_size or length > plan.file_size - offset:
            raise MiniDriverError("range exceeds object")
        stats = TransferStats()
        bytes_read = 0
        all_whole = True
        object_offset = 0
        end = offset + length
        for chunk in plan.chunks:
            chunk_end = object_offset + chunk.size
            begin, finish = max(offset, object_offset), min(end, chunk_end)
            if begin < finish:
                body, integrity = self._read_chunk(chunk, begin - object_offset, finish - begin, options, stats)
                sink(body)
                bytes_read += len(body)
                all_whole = all_whole and integrity is IntegrityStatus.VERIFIED_WHOLE_CHUNK
            object_offset = chunk_end
        if bytes_read != length:
            raise MiniDriverError("range length mismatch")
        integrity = IntegrityStatus.VERIFIED_WHOLE_CHUNK if all_whole else IntegrityStatus.UNVERIFIED_PARTIAL_RANGE
        return RangeReadResult(bytes_read, integrity), stats

    def head_object(self, reference: ObjectRef) -> ObjectInfo:
        self._validate_ref(reference)
        response = self._get_json(self._object_path(reference, "head"), control=True)
        returned = ObjectRef(self._required_str(response, "objectId"), self._required_int(response, "objectVersion"))
        if returned != reference:
            raise MiniDriverError("invalid object head response")
        return ObjectInfo(returned, int(response.get("metadataVersion", 0)), self._required_int(response, "fileSize"),
                          str(response.get("name", "")), str(response.get("parentPath", "")), self._required_str(response, "state"))

    def delete_object(self, reference: ObjectRef) -> None:
        self._validate_ref(reference)
        self._request("DELETE", self._object_path(reference, "delete"), None, control=True, expected=(202,))

    def get_object_read_hints(self, reference: ObjectRef) -> ObjectReadHints:
        self._validate_ref(reference)
        return self._decode_hints(self._post_json(self._object_path(reference, "read-hints"), {}, control=True), reference)

    def batch_get_object_read_hints(self, references: Iterable[ObjectRef]) -> list[ObjectReadHints]:
        refs = list(references)
        if not refs:
            raise ValueError("at least one ObjectRef is required")
        for reference in refs:
            self._validate_ref(reference)
        response = self._post_json("/internal/v3/objects/read-hints:batch", {
            "objects": [{"objectId": ref.object_id, "objectVersion": ref.object_version} for ref in refs],
        }, control=True)
        objects = response.get("objects")
        if not isinstance(objects, list) or len(objects) != len(refs):
            raise MiniDriverError("incomplete batch read-hints response")
        return [self._decode_hints(value, reference) for value, reference in zip(objects, refs)]

    def _create_session(self, size: int) -> dict:
        return self._post_json("/api/v2/upload/sessions", {
            "fileName": f"_object_{secrets.token_hex(16)}", "dirPath": "/", "fileSize": size,
        }, control=False)

    def _put_chunk(self, source: Path, file_size: int, session_id: str, chunk_size: int,
                   index: int, checksum_type: str) -> None:
        offset = index * chunk_size
        size = min(chunk_size, file_size - offset)
        with source.open("rb") as handle:
            handle.seek(offset)
            body = handle.read(size)
        if len(body) != size:
            raise MiniDriverError("cannot read upload source")
        digest = _checksum(checksum_type, body)
        route_key = digest if checksum_type == "sha256" else f"upload:{session_id}:{index}"
        route = None
        last_error: Exception | None = None
        for attempt in range(6):
            try:
                response = self._post_json(f"/api/v2/upload/sessions/{_escape(session_id)}/routes", {
                    "chunks": [{"index": index, "hash": route_key, "size": size,
                                "checksumType": checksum_type, "checksumDigest": digest}],
                }, control=False, retry_503=False)
                routes = response.get("routes")
                if not isinstance(routes, list) or len(routes) != 1:
                    raise MiniDriverError("invalid upload route")
                route = routes[0]
                break
            except MiniDriverError as error:
                last_error = error
                if "HTTP 503" not in str(error) or attempt == 5:
                    raise
                time.sleep(0.025 * (attempt + 1))
        if route is None:
            raise last_error or MiniDriverError("route admission stayed full")
        primary_address = self._required_str(route, "primaryAddress")
        primary_port = self._required_int(route, "primaryPort")
        primary_node_id = self._required_str(route, "primaryNodeId")
        token = self._required_str(route, "uploadToken")
        chain = route.get("chain")
        if not isinstance(chain, list) or not chain:
            raise MiniDriverError("invalid replica chain")
        encoded_chain = ";".join(
            f"{self._required_str(node, 'nodeId')}@{self._required_str(node, 'address')}:{self._required_int(node, 'httpPort')}"
            for node in chain
        )
        identity = str(route.get("storageIdentity") or route_key)
        gateway_port = self._gateway_parts.port or (443 if self._gateway_parts.scheme == "https" else 80)
        headers = {
            "Content-Type": "application/octet-stream", "X-Session-Id": session_id,
            "X-Chunk-Index": str(index), "X-Commit-Owner": primary_node_id,
            "X-Gateway-Address": self._gateway_parts.hostname or "",
            "X-Gateway-Port": str(gateway_port), "X-Replica-Chain": encoded_chain,
            "X-Replica-Position": "0", "X-Upload-Token": token,
        }
        self._direct_request("PUT", _endpoint(primary_address, primary_port, f"/v2/chunks/{_escape(identity)}"), body,
                             headers, expected=(200,))

    def _read_plan(self, reference: ObjectRef) -> _ReadPlan:
        self._validate_ref(reference)
        response = self._post_json(self._object_path(reference, "read-plan"), {}, control=True)
        returned = ObjectRef(self._required_str(response, "objectId"), self._required_int(response, "objectVersion"))
        file_size, chunk_size = self._required_int(response, "fileSize"), self._required_int(response, "chunkSize")
        raw_chunks = response.get("chunks")
        if returned != reference or file_size <= 0 or chunk_size <= 0 or not isinstance(raw_chunks, list) or not raw_chunks:
            raise MiniDriverError("invalid read-plan response")
        chunks: list[_ChunkPlan] = []
        for index, raw in enumerate(raw_chunks):
            replicas = raw.get("replicas")
            if not isinstance(replicas, list) or not replicas:
                raise MiniDriverError("invalid chunk replicas")
            chunk = _ChunkPlan(
                index=self._required_int(raw, "index"), storage_identity=self._required_str(raw, "storageIdentity"),
                size=self._required_int(raw, "size"), checksum_type=self._required_str(raw, "checksumType"),
                checksum_digest=self._required_str(raw, "checksumDigest"), read_capability=self._required_str(raw, "readCapability"),
                replicas=tuple(_Replica(self._required_str(replica, "nodeId"), self._required_str(replica, "address"),
                                        self._required_int(replica, "httpPort")) for replica in replicas),
            )
            if chunk.index != index or chunk.size <= 0:
                raise MiniDriverError("invalid chunk read-plan")
            chunks.append(chunk)
        return _ReadPlan(returned, file_size, chunk_size, tuple(chunks))

    def _read_chunk(self, chunk: _ChunkPlan, offset: int, length: int, options: ReadOptions,
                    stats: TransferStats) -> tuple[bytes, IntegrityStatus]:
        whole = offset == 0 and length == chunk.size
        attempts = min(len(chunk.replicas), max(1, options.max_replica_attempts if options.allow_replica_retry else 1))
        last_error: Exception | None = None
        for attempt, replica in enumerate(chunk.replicas[:attempts]):
            headers = {"X-Read-Token": chunk.read_capability}
            if not whole:
                headers["Range"] = f"bytes={offset}-{offset + length - 1}"
            try:
                body = self._direct_request("GET", _endpoint(replica.address, replica.http_port,
                                            f"/internal/v3/chunks/{_escape(chunk.storage_identity)}"), None, headers,
                                            expected=(200 if whole else 206,))
                stats.data_requests += 1
                if len(body) != length:
                    raise MiniDriverError("DataNode range length mismatch")
                if whole and not options.disable_checksum:
                    if _checksum(chunk.checksum_type, body) != chunk.checksum_digest:
                        raise MiniDriverError(f"chunk {chunk.checksum_type} mismatch")
                    stats.bytes_verified += len(body)
                    if attempt:
                        stats.replica_fallbacks += 1
                    return body, IntegrityStatus.VERIFIED_WHOLE_CHUNK
                if attempt:
                    stats.replica_fallbacks += 1
                return body, IntegrityStatus.UNVERIFIED_CHECKSUM_DISABLED if whole else IntegrityStatus.UNVERIFIED_PARTIAL_RANGE
            except (MiniDriverError, URLError) as error:
                last_error = error
        raise MiniDriverError(f"cannot read chunk {chunk.index}: {last_error}")

    def _decode_hints(self, response: dict, expected: ObjectRef) -> ObjectReadHints:
        returned = ObjectRef(self._required_str(response, "objectId"), self._required_int(response, "objectVersion"))
        if returned != expected:
            raise MiniDriverError("mismatched read-hints response")
        candidates = response.get("candidates")
        if not isinstance(candidates, list):
            raise MiniDriverError("invalid read-hints candidates")
        return ObjectReadHints(returned, self._required_int(response, "fileSize"), tuple(
            NodeReadHint(self._required_str(item, "nodeId"), self._required_int(item, "localBytes"),
                         self._required_int(item, "coveragePermille") / 1000.0, str(item.get("health", "unknown")))
            for item in candidates
        ))

    def _object_path(self, reference: ObjectRef, suffix: str) -> str:
        return f"/internal/v3/objects/{_escape(reference.object_id)}/versions/{reference.object_version}/{suffix}"

    def _get_json(self, path: str, control: bool) -> dict:
        body = self._request("GET", path, None, control=control, expected=(200,))
        return _json_object(body)

    def _post_json(self, path: str, value: dict, control: bool, retry_503: bool = False) -> dict:
        del retry_503  # retry policy is explicit in _put_chunk only.
        body = self._request("POST", path, json.dumps(value, separators=(",", ":")).encode(), control=control,
                             expected=(200, 201, 202))
        return _json_object(body)

    def _request(self, method: str, path: str, body: bytes | None, *, control: bool,
                 expected: tuple[int, ...]) -> bytes:
        headers = {"Content-Type": "application/json"} if body is not None else {}
        if control:
            headers.update({"X-Cluster-Internal-Token": self._config.cluster_internal_token,
                            "X-Service-Principal": self._config.service_principal})
        return self._direct_request(method, self._gateway + path, body, headers, expected=expected,
                                    timeout=self._config.gateway_timeout_seconds)

    def _direct_request(self, method: str, url: str, body: bytes | None, headers: dict[str, str], *,
                        expected: tuple[int, ...], timeout: float | None = None) -> bytes:
        request = Request(url, data=body, headers=headers, method=method)
        try:
            with self._opener.open(request, timeout=timeout if timeout is not None else self._config.datanode_timeout_seconds) as response:
                response_body = response.read()
                if response.status not in expected:
                    raise MiniDriverError(f"{method} {url} HTTP {response.status}: {response_body.decode(errors='replace').strip()}")
                return response_body
        except HTTPError as error:
            body = error.read().decode(errors="replace").strip()
            raise MiniDriverError(f"{method} {url} HTTP {error.code}: {body}") from error
        except URLError as error:
            raise MiniDriverError(f"{method} {url}: {error.reason}") from error

    @staticmethod
    def _required_str(value: dict, key: str) -> str:
        result = value.get(key)
        if not isinstance(result, str) or not result:
            raise MiniDriverError(f"invalid response field {key}")
        return result

    @staticmethod
    def _required_int(value: dict, key: str) -> int:
        result = value.get(key)
        if isinstance(result, bool) or not isinstance(result, int) or result < 0:
            raise MiniDriverError(f"invalid response field {key}")
        return result

    @staticmethod
    def _validate_ref(reference: ObjectRef) -> None:
        if not reference.object_id or reference.object_version <= 0:
            raise ValueError("ObjectRef requires object_id and positive object_version")


def _endpoint(address: str, port: int, path: str) -> str:
    host = f"[{address}]" if ":" in address and not address.startswith("[") else address
    return f"http://{host}:{port}{path}"


def _escape(value: str) -> str:
    return quote(value, safe="")


def _checksum(kind: str, body: bytes) -> str:
    if kind == "crc32c":
        return f"{_crc32c(body):08x}"
    if kind == "sha256":
        return hashlib.sha256(body).hexdigest()
    raise MiniDriverError(f"unsupported checksum type {kind}")


def _crc32c(body: bytes) -> int:
    """CRC-32C (Castagnoli), matching MiniDriver's crc32c protocol field."""
    value = 0xffffffff
    for byte in body:
        value = _CRC32C_TABLE[(value ^ byte) & 0xff] ^ (value >> 8)
    return value ^ 0xffffffff


def _make_crc32c_table() -> tuple[int, ...]:
    values: list[int] = []
    for index in range(256):
        value = index
        for _ in range(8):
            value = (value >> 1) ^ (0x82F63B78 if value & 1 else 0)
        values.append(value)
    return tuple(values)


_CRC32C_TABLE = _make_crc32c_table()


def _json_object(body: bytes) -> dict:
    try:
        value = json.loads(body)
    except json.JSONDecodeError as error:
        raise MiniDriverError("invalid JSON response") from error
    if not isinstance(value, dict):
        raise MiniDriverError("expected JSON object response")
    return value
