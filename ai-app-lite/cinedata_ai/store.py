from __future__ import annotations

import base64
import hashlib
import json
import math
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class Job:
    job_id: str
    kind: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class SearchPage:
    """A stable keyset page over one model-compatible vector query.

    SQLite still performs the cosine scan in Phase D Lite.  The cursor contract
    deliberately mirrors a future PostgreSQL/pgvector query: the caller never
    receives an SQL offset, and a page is bound to its vector, filters, model
    and a point-in-time upper bound for indexed assets.
    """

    results: list[dict[str, Any]]
    next_cursor: str | None
    total_candidates: int
    snapshot_updated_at: float


class CursorError(ValueError):
    """Raised when a query cursor is malformed or belongs to another query."""


class AssetStore:
    """Small SQLite-backed metadata and vector store for the first AI milestone.

    This intentionally keeps the storage contract close to a future pgvector table:
    metadata is JSON for the MVP, while vectors are serialized separately and can be
    migrated to a native vector column later.
    """

    def __init__(self, path: Path):
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._ready_asset_cache: tuple[dict[str, Any], ...] | None = None
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def _init_schema(self) -> None:
        with self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS assets (
                    asset_id TEXT PRIMARY KEY,
                    object_key TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    embedding_json TEXT,
                    embedding_provider TEXT,
                    embedding_model TEXT,
                    status TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS jobs_status_created
                    ON jobs(status, created_at);
                CREATE TABLE IF NOT EXISTS index_events (
                    event_id TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    object_version INTEGER NOT NULL,
                    job_id TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS index_targets (
                    object_id TEXT NOT NULL,
                    object_version INTEGER NOT NULL,
                    job_id TEXT NOT NULL,
                    PRIMARY KEY(object_id, object_version)
                );
                CREATE TABLE IF NOT EXISTS datasets (
                    dataset_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    owner TEXT NOT NULL,
                    purpose TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS datasets_owner_name
                    ON datasets(owner, name);
                CREATE TABLE IF NOT EXISTS dataset_members (
                    dataset_id TEXT NOT NULL,
                    asset_id TEXT NOT NULL,
                    object_version INTEGER NOT NULL,
                    role TEXT NOT NULL,
                    added_at REAL NOT NULL,
                    PRIMARY KEY(dataset_id, asset_id, object_version)
                );
                CREATE INDEX IF NOT EXISTS dataset_members_dataset
                    ON dataset_members(dataset_id, added_at);
                CREATE TABLE IF NOT EXISTS agent_runs (
                    run_id TEXT PRIMARY KEY,
                    caller TEXT NOT NULL,
                    query TEXT NOT NULL,
                    filters_json TEXT NOT NULL,
                    embedding_model TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS agent_run_results (
                    run_id TEXT NOT NULL,
                    asset_id TEXT NOT NULL,
                    object_version INTEGER NOT NULL,
                    rank INTEGER NOT NULL,
                    score REAL NOT NULL,
                    PRIMARY KEY(run_id, rank)
                );
                CREATE INDEX IF NOT EXISTS agent_run_results_run
                    ON agent_run_results(run_id, rank);
                """
            )
            columns = {row["name"] for row in db.execute("PRAGMA table_info(assets)")}
            if "embedding_model" not in columns:
                db.execute("ALTER TABLE assets ADD COLUMN embedding_model TEXT")

    def enqueue_asset(self, payload: dict[str, Any]) -> str:
        job_id = str(uuid.uuid4())
        now = time.time()
        with self._lock, self._connect() as db:
            db.execute(
                "INSERT INTO jobs(job_id, kind, payload_json, status, created_at, updated_at) "
                "VALUES (?, 'index_asset', ?, 'pending', ?, ?)",
                (job_id, json.dumps(payload, ensure_ascii=False), now, now),
            )
        return job_id

    def enqueue_committed_object(self, event: dict[str, Any]) -> str:
        """Persist a Redis FILE_UPLOAD_COMMITTED event before acknowledging it.

        `event_id` handles duplicate delivery of the exact Stream entry; the
        `(object_id, object_version)` target key additionally prevents a
        producer retry with a new event id from creating a duplicate index job.
        """
        event_id = str(event.get("eventId", "")).strip()
        object_id = str(event.get("objectId", "")).strip()
        file_hash = str(event.get("fileHash", "")).strip()
        try:
            object_version = int(event.get("objectVersion", 1))
            file_size = int(event.get("fileSize", 0))
            metadata_version = int(event.get("metadataVersion", 1))
        except (TypeError, ValueError) as exc:
            raise ValueError("objectVersion, metadataVersion and fileSize must be integers") from exc
        if (not event_id or not object_id or not file_hash or object_version <= 0 or
                metadata_version <= 0 or file_size <= 0):
            raise ValueError("invalid FILE_UPLOAD_COMMITTED event")

        now = time.time()
        with self._lock, self._connect() as db:
            existing = db.execute(
                "SELECT job_id FROM index_events WHERE event_id=?", (event_id,)
            ).fetchone()
            if existing is not None:
                return str(existing["job_id"])

            target = db.execute(
                "SELECT job_id FROM index_targets WHERE object_id=? AND object_version=?",
                (object_id, object_version),
            ).fetchone()
            if target is not None:
                job_id = str(target["job_id"])
                # A duplicate event is also a safe, explicit retry signal after
                # a previous local indexing failure.
                db.execute(
                    "UPDATE jobs SET status='pending', updated_at=?, last_error=NULL "
                    "WHERE job_id=? AND status='failed'",
                    (now, job_id),
                )
            else:
                job_id = str(uuid.uuid4())
                payload = {
                    "source": "minidrive",
                    "asset_id": object_id,
                    "object_key": str(event.get("objectKey", object_id)),
                    "file_hash": file_hash,
                    "file_size": file_size,
                    "object_version": object_version,
                    "metadata_version": metadata_version,
                    "event_id": event_id,
                }
                db.execute(
                    "INSERT INTO jobs(job_id, kind, payload_json, status, created_at, updated_at) "
                    "VALUES (?, 'index_asset', ?, 'pending', ?, ?)",
                    (job_id, json.dumps(payload, ensure_ascii=False), now, now),
                )
                db.execute(
                    "INSERT INTO index_targets(object_id, object_version, job_id) VALUES (?, ?, ?)",
                    (object_id, object_version, job_id),
                )
            db.execute(
                "INSERT INTO index_events(event_id, object_id, object_version, job_id, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (event_id, object_id, object_version, job_id, now),
            )
            return job_id

    def claim_job(self) -> Job | None:
        jobs = self.claim_jobs(1)
        return jobs[0] if jobs else None

    def claim_jobs(self, max_jobs: int) -> list[Job]:
        """Atomically claim a bounded FIFO batch for model inference."""
        if max_jobs <= 0:
            return []
        with self._lock, self._connect() as db:
            rows = db.execute(
                "SELECT job_id, kind, payload_json FROM jobs "
                "WHERE status = 'pending' ORDER BY created_at LIMIT ?",
                (max_jobs,),
            ).fetchall()
            claimed: list[Job] = []
            now = time.time()
            for row in rows:
                changed = db.execute(
                    "UPDATE jobs SET status='processing', attempts=attempts+1, updated_at=? "
                    "WHERE job_id=? AND status='pending'",
                    (now, row["job_id"]),
                ).rowcount
                if changed == 1:
                    claimed.append(Job(row["job_id"], row["kind"], json.loads(row["payload_json"])))
            return claimed

    def finish_job(self, job_id: str) -> None:
        with self._lock, self._connect() as db:
            db.execute(
                "UPDATE jobs SET status='done', updated_at=?, last_error=NULL WHERE job_id=?",
                (time.time(), job_id),
            )

    def fail_job(self, job_id: str, error: str) -> None:
        with self._lock, self._connect() as db:
            db.execute(
                "UPDATE jobs SET status='failed', updated_at=?, last_error=? WHERE job_id=?",
                (time.time(), error[:2000], job_id),
            )

    def job(self, job_id: str) -> dict[str, Any] | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["payload"] = json.loads(result.pop("payload_json"))
            return result

    def upsert_asset(
        self,
        asset_id: str,
        object_key: str,
        local_path: str,
        metadata: dict[str, Any],
        embedding: Iterable[float],
        provider: str,
        model: str,
    ) -> None:
        with self._lock, self._connect() as db:
            db.execute(
                "INSERT INTO assets(asset_id, object_key, local_path, metadata_json, "
                "embedding_json, embedding_provider, embedding_model, status, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'ready', ?) "
                "ON CONFLICT(asset_id) DO UPDATE SET object_key=excluded.object_key, "
                "local_path=excluded.local_path, metadata_json=excluded.metadata_json, "
                "embedding_json=excluded.embedding_json, embedding_provider=excluded.embedding_provider, "
                "embedding_model=excluded.embedding_model, "
                "status='ready', updated_at=excluded.updated_at",
                (
                    asset_id,
                    object_key,
                    local_path,
                    json.dumps(metadata, ensure_ascii=False, sort_keys=True),
                    json.dumps(list(embedding)),
                    provider,
                    model,
                    time.time(),
                ),
            )
            # SQLite remains authoritative.  The cache contains only immutable
            # ready-asset snapshots used by query threads and is refreshed
            # atomically after every completed indexing write.
            self._ready_asset_cache = None

    def get_asset(self, asset_id: str) -> dict[str, Any] | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM assets WHERE asset_id=?", (asset_id,)).fetchone()
            return self._asset_row(row) if row else None

    def all_assets(self) -> list[dict[str, Any]]:
        with self._connect() as db:
            return [self._asset_row(row) for row in db.execute("SELECT * FROM assets")]

    def create_dataset(self, name: str, owner: str, purpose: str) -> dict[str, Any]:
        """Create a named, user-owned collection of immutable asset versions."""
        normalized_name = str(name).strip()
        normalized_owner = str(owner).strip()
        normalized_purpose = str(purpose).strip()
        if not normalized_name or not normalized_owner or not normalized_purpose:
            raise ValueError("dataset name, owner and purpose are required")
        if len(normalized_name) > 200 or len(normalized_owner) > 200 or len(normalized_purpose) > 2000:
            raise ValueError("dataset field exceeds length limit")
        record = {
            "dataset_id": str(uuid.uuid4()),
            "name": normalized_name,
            "owner": normalized_owner,
            "purpose": normalized_purpose,
            "created_at": time.time(),
        }
        with self._lock, self._connect() as db:
            try:
                db.execute(
                    "INSERT INTO datasets(dataset_id, name, owner, purpose, created_at) VALUES (?, ?, ?, ?, ?)",
                    (record["dataset_id"], record["name"], record["owner"], record["purpose"], record["created_at"]),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError("dataset name already exists for this owner") from exc
        return record

    def list_datasets(self, owner: str | None = None) -> list[dict[str, Any]]:
        with self._connect() as db:
            if owner:
                rows = db.execute(
                    "SELECT dataset_id, name, owner, purpose, created_at FROM datasets WHERE owner=? "
                    "ORDER BY created_at DESC, dataset_id", (owner,)
                ).fetchall()
            else:
                rows = db.execute(
                    "SELECT dataset_id, name, owner, purpose, created_at FROM datasets "
                    "ORDER BY created_at DESC, dataset_id"
                ).fetchall()
        return [dict(row) for row in rows]

    def dataset(self, dataset_id: str) -> dict[str, Any] | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT dataset_id, name, owner, purpose, created_at FROM datasets WHERE dataset_id=?",
                (dataset_id,),
            ).fetchone()
            if row is None:
                return None
            members = db.execute(
                "SELECT dataset_id, asset_id, object_version, role, added_at FROM dataset_members "
                "WHERE dataset_id=? ORDER BY added_at, asset_id, object_version", (dataset_id,)
            ).fetchall()
        result = dict(row)
        result["members"] = [dict(member) for member in members]
        return result

    def add_dataset_members(self, dataset_id: str, asset_ids: Iterable[str], role: str = "train") -> list[dict[str, Any]]:
        """Snapshot each selected ready asset's current object version.

        A later re-index of the same asset may update metadata/object_version,
        but it never mutates this dataset membership.  Repeating the same
        request is idempotent at the (dataset, asset, version) level.
        """
        normalized_role = str(role).strip()
        ids = list(dict.fromkeys(str(asset_id).strip() for asset_id in asset_ids if str(asset_id).strip()))
        if not ids:
            raise ValueError("asset_ids is required")
        if not normalized_role or len(normalized_role) > 64:
            raise ValueError("invalid dataset member role")
        now = time.time()
        members: list[dict[str, Any]] = []
        with self._lock, self._connect() as db:
            if db.execute("SELECT 1 FROM datasets WHERE dataset_id=?", (dataset_id,)).fetchone() is None:
                raise KeyError("dataset not found")
            for asset_id in ids:
                row = db.execute(
                    "SELECT metadata_json, status FROM assets WHERE asset_id=?", (asset_id,)
                ).fetchone()
                if row is None or row["status"] != "ready":
                    raise ValueError(f"asset is not ready: {asset_id}")
                metadata = json.loads(row["metadata_json"])
                try:
                    object_version = int(metadata.get("object_version", 1))
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"invalid object_version for asset: {asset_id}") from exc
                if object_version <= 0:
                    raise ValueError(f"invalid object_version for asset: {asset_id}")
                db.execute(
                    "INSERT OR IGNORE INTO dataset_members(dataset_id, asset_id, object_version, role, added_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (dataset_id, asset_id, object_version, normalized_role, now),
                )
                member = db.execute(
                    "SELECT dataset_id, asset_id, object_version, role, added_at FROM dataset_members "
                    "WHERE dataset_id=? AND asset_id=? AND object_version=?",
                    (dataset_id, asset_id, object_version),
                ).fetchone()
                members.append(dict(member))
        return members

    def record_agent_search(self, caller: str, query: str, filters: dict[str, Any], embedding_model: str,
                            results: Iterable[dict[str, Any]]) -> dict[str, Any]:
        """Persist read-only Agent evidence without granting it write authority."""
        normalized_caller = str(caller).strip()
        if not normalized_caller:
            raise ValueError("caller is required")
        run = {
            "run_id": str(uuid.uuid4()),
            "caller": normalized_caller,
            "query": str(query),
            "filters": dict(filters),
            "embedding_model": str(embedding_model),
            "created_at": time.time(),
        }
        with self._lock, self._connect() as db:
            db.execute(
                "INSERT INTO agent_runs(run_id, caller, query, filters_json, embedding_model, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (run["run_id"], run["caller"], run["query"], json.dumps(run["filters"], ensure_ascii=False,
                 sort_keys=True), run["embedding_model"], run["created_at"]),
            )
            for rank, result in enumerate(results, start=1):
                metadata = result.get("metadata") or {}
                try:
                    object_version = int(result.get("object_version", metadata.get("object_version", 1)))
                except (TypeError, ValueError) as exc:
                    raise ValueError("invalid result object_version") from exc
                db.execute(
                    "INSERT INTO agent_run_results(run_id, asset_id, object_version, rank, score) VALUES (?, ?, ?, ?, ?)",
                    (run["run_id"], str(result["asset_id"]), object_version, rank, float(result.get("score", 0.0))),
                )
        return self.agent_run(run["run_id"]) or run

    def agent_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM agent_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                return None
            evidence = db.execute(
                "SELECT asset_id, object_version, rank, score FROM agent_run_results WHERE run_id=? ORDER BY rank",
                (run_id,),
            ).fetchall()
        result = dict(row)
        result["filters"] = json.loads(result.pop("filters_json"))
        result["evidence"] = [dict(item) for item in evidence]
        return result

    @staticmethod
    def _asset_row(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["metadata"] = json.loads(result.pop("metadata_json"))
        result["embedding"] = json.loads(result.pop("embedding_json")) if result["embedding_json"] else []
        result.pop("embedding_json", None)
        return result

    def search(
        self,
        query_vector: Iterable[float],
        top_k: int = 10,
        metadata_filter: dict[str, Any] | None = None,
        embedding_model: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.search_page(
            query_vector, page_size=top_k, metadata_filter=metadata_filter,
            embedding_model=embedding_model,
        ).results

    def search_page(
        self,
        query_vector: Iterable[float],
        page_size: int = 10,
        metadata_filter: dict[str, Any] | None = None,
        embedding_model: str | None = None,
        cursor: str | None = None,
    ) -> SearchPage:
        """Return one deterministic keyset page for a vector/metadata query.

        `offset` pagination becomes unstable when new objects are indexed
        between requests.  The cursor therefore keeps the final score/id from
        the previous page and an `updated_at` snapshot ceiling.  It is an API
        contract, not a security token; authorization belongs to the future
        Gateway/Dataset layer.
        """
        query = [float(value) for value in query_vector]
        if not query:
            raise ValueError("query vector must not be empty")
        filters = dict(metadata_filter or {})
        size = max(1, min(int(page_size), 50))
        fingerprint = self._query_fingerprint(query, filters, embedding_model)
        cursor_state = self._decode_cursor(cursor) if cursor else None
        if cursor_state is not None and cursor_state.get("fingerprint") != fingerprint:
            raise CursorError("cursor does not belong to this query")
        snapshot_updated_at = (
            float(cursor_state["snapshot_updated_at"])
            if cursor_state is not None else time.time()
        )
        last_score = float(cursor_state["last_score"]) if cursor_state is not None else None
        last_asset_id = str(cursor_state["last_asset_id"]) if cursor_state is not None else None

        candidates = self._ready_asset_snapshot()
        scored: list[dict[str, Any]] = []
        for cached in candidates:
            asset = dict(cached)
            if float(asset.get("updated_at", 0.0)) > snapshot_updated_at:
                continue
            if embedding_model and asset.get("embedding_model") != embedding_model:
                continue
            if not self._matches(asset, filters):
                continue
            asset["score"] = _cosine(query, asset["embedding"])
            scored.append(asset)
        scored.sort(key=lambda item: (-float(item["score"]), str(item["asset_id"])))

        if last_score is not None and last_asset_id is not None:
            scored = [
                item for item in scored
                if float(item["score"]) < last_score
                or (float(item["score"]) == last_score and str(item["asset_id"]) > last_asset_id)
            ]
        total_candidates = self._candidate_count(candidates, filters, embedding_model, snapshot_updated_at)
        page = scored[:size]
        next_cursor = None
        if len(scored) > len(page) and page:
            tail = page[-1]
            next_cursor = self._encode_cursor({
                "v": 1,
                "fingerprint": fingerprint,
                "snapshot_updated_at": snapshot_updated_at,
                "last_score": float(tail["score"]),
                "last_asset_id": str(tail["asset_id"]),
            })
        return SearchPage(page, next_cursor, total_candidates, snapshot_updated_at)

    def index_stats(self) -> dict[str, Any]:
        """Small observability summary used by the Phase D Lite metrics API."""
        with self._connect() as db:
            ready = db.execute(
                "SELECT COUNT(*) AS count, MAX(updated_at) AS newest FROM assets WHERE status='ready'"
            ).fetchone()
            jobs = {
                str(row["status"]): int(row["count"])
                for row in db.execute("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status")
            }
        newest = float(ready["newest"]) if ready and ready["newest"] is not None else None
        return {
            "ready_assets": int(ready["count"]) if ready else 0,
            "newest_indexed_at": newest,
            "index_freshness_seconds": max(0.0, time.time() - newest) if newest is not None else None,
            "jobs_by_status": jobs,
        }

    @staticmethod
    def _matches(asset: dict[str, Any], filters: dict[str, Any]) -> bool:
        metadata = asset["metadata"]
        for key, expected in filters.items():
            if expected in (None, ""):
                continue
            if key == "path_prefix":
                if not str(asset["object_key"]).startswith(str(expected)):
                    return False
                continue
            if key == "semantic_tags":
                expected_tags = expected if isinstance(expected, list) else [expected]
                actual_tags = [str(tag).lower() for tag in metadata.get("semantic_tags", [])]
                if not any(str(tag).lower() in actual_tags for tag in expected_tags):
                    return False
                continue
            if key == "quality_min":
                try:
                    if float(metadata.get("quality_score", 0.0)) < float(expected):
                        return False
                except (TypeError, ValueError):
                    return False
                continue
            if key == "capture_time_from":
                if str(metadata.get("capture_time", "")) < str(expected):
                    return False
                continue
            if key == "capture_time_to":
                if str(metadata.get("capture_time", "")) > str(expected):
                    return False
                continue
            actual = metadata.get(key)
            if isinstance(expected, list):
                if actual not in expected:
                    return False
            elif str(expected).lower() not in str(actual).lower():
                return False
        return True

    def _candidate_count(self, candidates: tuple[dict[str, Any], ...], filters: dict[str, Any],
                         embedding_model: str | None, snapshot_updated_at: float) -> int:
        return sum(
            1
            for asset in candidates
            if float(asset.get("updated_at", 0.0)) <= snapshot_updated_at
            and (not embedding_model or asset.get("embedding_model") == embedding_model)
            and self._matches(asset, filters)
        )

    def _ready_asset_snapshot(self) -> tuple[dict[str, Any], ...]:
        """Load ready vectors once, then let concurrent reads use one snapshot.

        Without this cache every local query opened SQLite and decoded every
        vector JSON again.  The bounded demo does not implement an invalidation
        bus across processes; indexing writes in this process invalidate the
        snapshot, while the future PostgreSQL/pgvector service replaces this
        optimization with database-side filtering and ANN retrieval.
        """
        with self._lock:
            if self._ready_asset_cache is None:
                with self._connect() as db:
                    rows = db.execute("SELECT * FROM assets WHERE status='ready'").fetchall()
                self._ready_asset_cache = tuple(self._asset_row(row) for row in rows)
            return self._ready_asset_cache

    @staticmethod
    def _query_fingerprint(query: list[float], filters: dict[str, Any], embedding_model: str | None) -> str:
        canonical = json.dumps(
            {"vector": query, "filters": filters, "embedding_model": embedding_model},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _encode_cursor(payload: dict[str, Any]) -> str:
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    @staticmethod
    def _decode_cursor_payload(value: str) -> dict[str, Any]:
        try:
            padded = value + "=" * (-len(value) % 4)
            parsed = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
        except (UnicodeEncodeError, ValueError, json.JSONDecodeError) as exc:
            raise CursorError("invalid cursor") from exc
        if not isinstance(parsed, dict):
            raise CursorError("invalid cursor")
        return parsed

    @staticmethod
    def _decode_cursor(value: str) -> dict[str, Any]:
        parsed = AssetStore._decode_cursor_payload(value)
        required = {"v", "fingerprint", "snapshot_updated_at", "last_score", "last_asset_id"}
        if parsed.get("v") != 1 or not required.issubset(parsed):
            raise CursorError("invalid cursor")
        return parsed


def _cosine(left: list[float], right: list[float]) -> float:
    if len(left) != len(right) or not left:
        return 0.0
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    return dot / (left_norm * right_norm) if left_norm and right_norm else 0.0
