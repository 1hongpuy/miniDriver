from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Iterable

from .store import CursorError, Job, SearchPage


class PostgresAssetStore:
    """PostgreSQL + pgvector implementation of the AI repository contract.

    PostgreSQL is the Phase D authoritative service: transactions protect Job
    claiming/event de-duplication, JSONB provides metadata filters, and
    pgvector computes cosine distance in SQL.  It intentionally has no
    in-process asset cache, so API/worker processes observe the same committed
    data.  SQLite's AssetStore remains an offline fallback only.
    """

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._init_schema()

    def _connect(self) -> Any:
        try:
            import psycopg  # type: ignore
            from psycopg.rows import dict_row  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "PostgreSQL backend requires psycopg; install with "
                "pip install -r ai-app-lite/requirements-postgres.txt"
            ) from exc
        return psycopg.connect(self.dsn, row_factory=dict_row)

    def _init_schema(self) -> None:
        statements = (
            "CREATE EXTENSION IF NOT EXISTS vector",
            "CREATE SEQUENCE IF NOT EXISTS ai_asset_generation_seq",
            """
            CREATE TABLE IF NOT EXISTS assets (
                asset_id TEXT PRIMARY KEY,
                object_key TEXT NOT NULL,
                local_path TEXT NOT NULL,
                metadata_json JSONB NOT NULL,
                embedding vector,
                embedding_provider TEXT,
                embedding_model TEXT,
                status TEXT NOT NULL,
                updated_at DOUBLE PRECISION NOT NULL,
                index_generation BIGINT NOT NULL DEFAULT nextval('ai_asset_generation_seq')
            )
            """,
            "CREATE INDEX IF NOT EXISTS assets_ready_model_generation ON assets(status, embedding_model, index_generation)",
            "CREATE INDEX IF NOT EXISTS assets_metadata_gin ON assets USING GIN(metadata_json)",
            """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                payload_json JSONB NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at DOUBLE PRECISION NOT NULL,
                updated_at DOUBLE PRECISION NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS jobs_status_created ON jobs(status, created_at)",
            """
            CREATE TABLE IF NOT EXISTS index_events (
                event_id TEXT PRIMARY KEY,
                object_id TEXT NOT NULL,
                object_version BIGINT NOT NULL,
                job_id TEXT NOT NULL,
                created_at DOUBLE PRECISION NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS index_targets (
                object_id TEXT NOT NULL,
                object_version BIGINT NOT NULL,
                job_id TEXT NOT NULL,
                PRIMARY KEY(object_id, object_version)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS datasets (
                dataset_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                owner TEXT NOT NULL,
                purpose TEXT NOT NULL,
                created_at DOUBLE PRECISION NOT NULL,
                UNIQUE(owner, name)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dataset_members (
                dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
                asset_id TEXT NOT NULL,
                object_version BIGINT NOT NULL,
                role TEXT NOT NULL,
                added_at DOUBLE PRECISION NOT NULL,
                PRIMARY KEY(dataset_id, asset_id, object_version)
            )
            """,
            "CREATE INDEX IF NOT EXISTS dataset_members_dataset ON dataset_members(dataset_id, added_at)",
            """
            CREATE TABLE IF NOT EXISTS agent_runs (
                run_id TEXT PRIMARY KEY,
                caller TEXT NOT NULL,
                query TEXT NOT NULL,
                filters_json JSONB NOT NULL,
                embedding_model TEXT NOT NULL,
                created_at DOUBLE PRECISION NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS agent_run_results (
                run_id TEXT NOT NULL REFERENCES agent_runs(run_id) ON DELETE CASCADE,
                asset_id TEXT NOT NULL,
                object_version BIGINT NOT NULL,
                rank INTEGER NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                PRIMARY KEY(run_id, rank)
            )
            """,
            "CREATE INDEX IF NOT EXISTS agent_run_results_run ON agent_run_results(run_id, rank)",
        )
        with self._connect() as db:
            for statement in statements:
                db.execute(statement)

    def enqueue_asset(self, payload: dict[str, Any]) -> str:
        job_id = str(uuid.uuid4())
        now = time.time()
        with self._connect() as db:
            db.execute(
                "INSERT INTO jobs(job_id, kind, payload_json, status, created_at, updated_at) "
                "VALUES (%s, 'index_asset', %s::jsonb, 'pending', %s, %s)",
                (job_id, self._json(payload), now, now),
            )
        return job_id

    def enqueue_committed_object(self, event: dict[str, Any]) -> str:
        event_id = str(event.get("eventId", "")).strip()
        object_id = str(event.get("objectId", "")).strip()
        file_hash = str(event.get("fileHash", "")).strip()
        try:
            object_version = int(event.get("objectVersion", 1))
            file_size = int(event.get("fileSize", 0))
            metadata_version = int(event.get("metadataVersion", 1))
        except (TypeError, ValueError) as exc:
            raise ValueError("objectVersion, metadataVersion and fileSize must be integers") from exc
        if not event_id or not object_id or not file_hash or object_version <= 0 or metadata_version <= 0 or file_size <= 0:
            raise ValueError("invalid FILE_UPLOAD_COMMITTED event")

        now = time.time()
        with self._connect() as db:
            existing = db.execute(
                "SELECT job_id FROM index_events WHERE event_id=%s FOR UPDATE", (event_id,)
            ).fetchone()
            if existing is not None:
                return str(existing["job_id"])
            target = db.execute(
                "SELECT job_id FROM index_targets WHERE object_id=%s AND object_version=%s FOR UPDATE",
                (object_id, object_version),
            ).fetchone()
            if target is not None:
                job_id = str(target["job_id"])
                db.execute(
                    "UPDATE jobs SET status='pending', updated_at=%s, last_error=NULL "
                    "WHERE job_id=%s AND status='failed'", (now, job_id),
                )
            else:
                job_id = str(uuid.uuid4())
                payload = {
                    "source": "minidrive", "asset_id": object_id,
                    "object_key": str(event.get("objectKey", object_id)), "file_hash": file_hash,
                    "file_size": file_size, "object_version": object_version,
                    "metadata_version": metadata_version, "event_id": event_id,
                }
                db.execute(
                    "INSERT INTO jobs(job_id, kind, payload_json, status, created_at, updated_at) "
                    "VALUES (%s, 'index_asset', %s::jsonb, 'pending', %s, %s)",
                    (job_id, self._json(payload), now, now),
                )
                db.execute(
                    "INSERT INTO index_targets(object_id, object_version, job_id) VALUES (%s, %s, %s)",
                    (object_id, object_version, job_id),
                )
            db.execute(
                "INSERT INTO index_events(event_id, object_id, object_version, job_id, created_at) "
                "VALUES (%s, %s, %s, %s, %s)", (event_id, object_id, object_version, job_id, now),
            )
            return job_id

    def claim_job(self) -> Job | None:
        jobs = self.claim_jobs(1)
        return jobs[0] if jobs else None

    def claim_jobs(self, max_jobs: int) -> list[Job]:
        if max_jobs <= 0:
            return []
        with self._connect() as db:
            rows = db.execute(
                "SELECT job_id, kind, payload_json FROM jobs WHERE status='pending' "
                "ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT %s", (max_jobs,)
            ).fetchall()
            if not rows:
                return []
            now = time.time()
            ids = [row["job_id"] for row in rows]
            db.execute(
                "UPDATE jobs SET status='processing', attempts=attempts+1, updated_at=%s "
                "WHERE job_id = ANY(%s::text[])", (now, ids),
            )
            return [Job(str(row["job_id"]), str(row["kind"]), self._decode_json(row["payload_json"])) for row in rows]

    def finish_job(self, job_id: str) -> None:
        with self._connect() as db:
            db.execute("UPDATE jobs SET status='done', updated_at=%s, last_error=NULL WHERE job_id=%s",
                       (time.time(), job_id))

    def fail_job(self, job_id: str, error: str) -> None:
        with self._connect() as db:
            db.execute("UPDATE jobs SET status='failed', updated_at=%s, last_error=%s WHERE job_id=%s",
                       (time.time(), error[:2000], job_id))

    def job(self, job_id: str) -> dict[str, Any] | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE job_id=%s", (job_id,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["payload"] = self._decode_json(result.pop("payload_json"))
        return result

    def upsert_asset(self, asset_id: str, object_key: str, local_path: str, metadata: dict[str, Any],
                     embedding: Iterable[float], provider: str, model: str) -> None:
        now = time.time()
        with self._connect() as db:
            db.execute(
                "INSERT INTO assets(asset_id, object_key, local_path, metadata_json, embedding, "
                "embedding_provider, embedding_model, status, updated_at, index_generation) "
                "VALUES (%s, %s, %s, %s::jsonb, %s::vector, %s, %s, 'ready', %s, nextval('ai_asset_generation_seq')) "
                "ON CONFLICT(asset_id) DO UPDATE SET object_key=excluded.object_key, "
                "local_path=excluded.local_path, metadata_json=excluded.metadata_json, embedding=excluded.embedding, "
                "embedding_provider=excluded.embedding_provider, embedding_model=excluded.embedding_model, "
                "status='ready', updated_at=excluded.updated_at, index_generation=nextval('ai_asset_generation_seq')",
                (asset_id, object_key, local_path, self._json(metadata), self._vector(embedding), provider, model, now),
            )

    def get_asset(self, asset_id: str) -> dict[str, Any] | None:
        with self._connect() as db:
            row = db.execute(self._asset_select() + " WHERE asset_id=%s", (asset_id,)).fetchone()
        return self._asset_row(row) if row else None

    def all_assets(self) -> list[dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute(self._asset_select() + " ORDER BY asset_id").fetchall()
        return [self._asset_row(row) for row in rows]

    def create_dataset(self, name: str, owner: str, purpose: str) -> dict[str, Any]:
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
        try:
            with self._connect() as db:
                db.execute(
                    "INSERT INTO datasets(dataset_id, name, owner, purpose, created_at) VALUES (%s, %s, %s, %s, %s)",
                    (record["dataset_id"], record["name"], record["owner"], record["purpose"], record["created_at"]),
                )
        except Exception as exc:
            if self._is_unique_violation(exc):
                raise ValueError("dataset name already exists for this owner") from exc
            raise
        return record

    def list_datasets(self, owner: str | None = None) -> list[dict[str, Any]]:
        with self._connect() as db:
            if owner:
                rows = db.execute(
                    "SELECT dataset_id, name, owner, purpose, created_at FROM datasets WHERE owner=%s "
                    "ORDER BY created_at DESC, dataset_id", (owner,)
                ).fetchall()
            else:
                rows = db.execute(
                    "SELECT dataset_id, name, owner, purpose, created_at FROM datasets ORDER BY created_at DESC, dataset_id"
                ).fetchall()
        return [dict(row) for row in rows]

    def dataset(self, dataset_id: str) -> dict[str, Any] | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT dataset_id, name, owner, purpose, created_at FROM datasets WHERE dataset_id=%s", (dataset_id,)
            ).fetchone()
            if row is None:
                return None
            members = db.execute(
                "SELECT dataset_id, asset_id, object_version, role, added_at FROM dataset_members "
                "WHERE dataset_id=%s ORDER BY added_at, asset_id, object_version", (dataset_id,)
            ).fetchall()
        result = dict(row)
        result["members"] = [dict(member) for member in members]
        return result

    def add_dataset_members(self, dataset_id: str, asset_ids: Iterable[str], role: str = "train") -> list[dict[str, Any]]:
        normalized_role = str(role).strip()
        ids = list(dict.fromkeys(str(asset_id).strip() for asset_id in asset_ids if str(asset_id).strip()))
        if not ids:
            raise ValueError("asset_ids is required")
        if not normalized_role or len(normalized_role) > 64:
            raise ValueError("invalid dataset member role")
        now = time.time()
        members: list[dict[str, Any]] = []
        with self._connect() as db:
            exists = db.execute("SELECT 1 FROM datasets WHERE dataset_id=%s FOR SHARE", (dataset_id,)).fetchone()
            if exists is None:
                raise KeyError("dataset not found")
            for asset_id in ids:
                row = db.execute(
                    "SELECT metadata_json, status FROM assets WHERE asset_id=%s FOR SHARE", (asset_id,)
                ).fetchone()
                if row is None or row["status"] != "ready":
                    raise ValueError(f"asset is not ready: {asset_id}")
                metadata = self._decode_json(row["metadata_json"])
                try:
                    object_version = int(metadata.get("object_version", 1))
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"invalid object_version for asset: {asset_id}") from exc
                if object_version <= 0:
                    raise ValueError(f"invalid object_version for asset: {asset_id}")
                db.execute(
                    "INSERT INTO dataset_members(dataset_id, asset_id, object_version, role, added_at) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT(dataset_id, asset_id, object_version) DO NOTHING",
                    (dataset_id, asset_id, object_version, normalized_role, now),
                )
                member = db.execute(
                    "SELECT dataset_id, asset_id, object_version, role, added_at FROM dataset_members "
                    "WHERE dataset_id=%s AND asset_id=%s AND object_version=%s",
                    (dataset_id, asset_id, object_version),
                ).fetchone()
                members.append(dict(member))
        return members

    def record_agent_search(self, caller: str, query: str, filters: dict[str, Any], embedding_model: str,
                            results: Iterable[dict[str, Any]]) -> dict[str, Any]:
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
        with self._connect() as db:
            db.execute(
                "INSERT INTO agent_runs(run_id, caller, query, filters_json, embedding_model, created_at) "
                "VALUES (%s, %s, %s, %s::jsonb, %s, %s)",
                (run["run_id"], run["caller"], run["query"], self._json(run["filters"]),
                 run["embedding_model"], run["created_at"]),
            )
            for rank, result in enumerate(results, start=1):
                metadata = result.get("metadata") or {}
                try:
                    object_version = int(result.get("object_version", metadata.get("object_version", 1)))
                except (TypeError, ValueError) as exc:
                    raise ValueError("invalid result object_version") from exc
                db.execute(
                    "INSERT INTO agent_run_results(run_id, asset_id, object_version, rank, score) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (run["run_id"], str(result["asset_id"]), object_version, rank, float(result.get("score", 0.0))),
                )
        return self.agent_run(run["run_id"]) or run

    def agent_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM agent_runs WHERE run_id=%s", (run_id,)).fetchone()
            if row is None:
                return None
            evidence = db.execute(
                "SELECT asset_id, object_version, rank, score FROM agent_run_results WHERE run_id=%s ORDER BY rank",
                (run_id,),
            ).fetchall()
        result = dict(row)
        result["filters"] = self._decode_json(result.pop("filters_json"))
        result["evidence"] = [dict(item) for item in evidence]
        return result

    def search(self, query_vector: Iterable[float], top_k: int = 10,
               metadata_filter: dict[str, Any] | None = None,
               embedding_model: str | None = None) -> list[dict[str, Any]]:
        return self.search_page(query_vector, top_k, metadata_filter, embedding_model).results

    def search_page(self, query_vector: Iterable[float], page_size: int = 10,
                    metadata_filter: dict[str, Any] | None = None,
                    embedding_model: str | None = None, cursor: str | None = None) -> SearchPage:
        query = [float(value) for value in query_vector]
        if not query:
            raise ValueError("query vector must not be empty")
        filters = dict(metadata_filter or {})
        size = max(1, min(int(page_size), 50))
        fingerprint = self._fingerprint(query, filters, embedding_model)
        cursor_state = self._decode_cursor(cursor) if cursor else None
        if cursor_state is not None and cursor_state.get("fingerprint") != fingerprint:
            raise CursorError("cursor does not belong to this query")

        where, filter_params = self._where(filters, embedding_model)
        with self._connect() as db:
            if cursor_state is None:
                snapshot_row = db.execute(
                    "SELECT COALESCE(MAX(index_generation), 0) AS generation FROM assets WHERE " + " AND ".join(where),
                    filter_params,
                ).fetchone()
                snapshot_generation = int(snapshot_row["generation"])
            else:
                snapshot_generation = int(cursor_state["snapshot_generation"])
            stable_where = where + ["index_generation <= %s"]
            stable_params = [*filter_params, snapshot_generation]
            total = db.execute(
                "SELECT COUNT(*) AS count FROM assets WHERE " + " AND ".join(stable_where), stable_params
            ).fetchone()
            ranked_sql = (
                "WITH ranked AS (SELECT asset_id, object_key, local_path, metadata_json, embedding::text AS embedding_text, "
                "embedding_provider, embedding_model, status, updated_at, index_generation, "
                "embedding <=> %s::vector AS distance FROM assets WHERE " + " AND ".join(stable_where) + ") "
                "SELECT *, 1.0 - distance AS score FROM ranked"
            )
            params: list[Any] = [self._vector(query), *stable_params]
            if cursor_state is not None:
                ranked_sql += " WHERE distance > %s OR (distance = %s AND asset_id > %s)"
                params.extend([float(cursor_state["last_distance"]), float(cursor_state["last_distance"]),
                               str(cursor_state["last_asset_id"])])
            ranked_sql += " ORDER BY distance ASC, asset_id ASC LIMIT %s"
            params.append(size + 1)
            rows = db.execute(ranked_sql, params).fetchall()
        has_more = len(rows) > size
        page_rows = rows[:size]
        results = [self._asset_row(row) | {"score": float(row["score"])} for row in page_rows]
        next_cursor = None
        if has_more and page_rows:
            tail = page_rows[-1]
            next_cursor = self._encode_cursor({
                "v": 2, "fingerprint": fingerprint, "snapshot_generation": snapshot_generation,
                "last_distance": float(tail["distance"]), "last_asset_id": str(tail["asset_id"]),
            })
        # The API field was introduced before PostgreSQL.  Keep it compatible,
        # but make its integer generation meaning explicit in the documentation.
        return SearchPage(results, next_cursor, int(total["count"]), float(snapshot_generation))

    def index_stats(self) -> dict[str, Any]:
        with self._connect() as db:
            assets = db.execute(
                "SELECT COUNT(*) AS count, MAX(updated_at) AS newest, MAX(index_generation) AS generation "
                "FROM assets WHERE status='ready'"
            ).fetchone()
            jobs = {str(row["status"]): int(row["count"])
                    for row in db.execute("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status")}
        newest = float(assets["newest"]) if assets and assets["newest"] is not None else None
        return {
            "ready_assets": int(assets["count"]) if assets else 0,
            "newest_indexed_at": newest,
            "index_freshness_seconds": max(0.0, time.time() - newest) if newest is not None else None,
            "index_generation": int(assets["generation"] or 0) if assets else 0,
            "jobs_by_status": jobs,
        }

    @staticmethod
    def _asset_select() -> str:
        return ("SELECT asset_id, object_key, local_path, metadata_json, embedding::text AS embedding_text, "
                "embedding_provider, embedding_model, status, updated_at, index_generation FROM assets")

    @classmethod
    def _asset_row(cls, row: dict[str, Any]) -> dict[str, Any]:
        result = dict(row)
        result["metadata"] = cls._decode_json(result.pop("metadata_json"))
        result["embedding"] = cls._parse_vector(result.pop("embedding_text", None))
        return result

    @staticmethod
    def _where(filters: dict[str, Any], embedding_model: str | None) -> tuple[list[str], list[Any]]:
        clauses = ["status='ready'", "embedding IS NOT NULL"]
        params: list[Any] = []
        if embedding_model:
            clauses.append("embedding_model=%s")
            params.append(embedding_model)
        for key, expected in filters.items():
            if expected in (None, ""):
                continue
            if key == "path_prefix":
                clauses.append("object_key LIKE %s")
                params.append(str(expected) + "%")
            elif key == "semantic_tags":
                values = expected if isinstance(expected, list) else [expected]
                clauses.append("metadata_json -> 'semantic_tags' ?| %s::text[]")
                params.append([str(value) for value in values])
            elif key == "quality_min":
                clauses.append("COALESCE((metadata_json ->> 'quality_score')::double precision, 0) >= %s")
                params.append(float(expected))
            elif key == "capture_time_from":
                clauses.append("COALESCE(metadata_json ->> 'capture_time', '') >= %s")
                params.append(str(expected))
            elif key == "capture_time_to":
                clauses.append("COALESCE(metadata_json ->> 'capture_time', '') <= %s")
                params.append(str(expected))
            elif isinstance(expected, list):
                clauses.append("metadata_json ->> %s = ANY(%s::text[])")
                params.extend([key, [str(value) for value in expected]])
            else:
                clauses.append("COALESCE(metadata_json ->> %s, '') ILIKE %s")
                params.extend([key, "%" + str(expected) + "%"])
        return clauses, params

    @staticmethod
    def _vector(values: Iterable[float]) -> str:
        return "[" + ",".join(format(float(value), ".12g") for value in values) + "]"

    @staticmethod
    def _parse_vector(value: Any) -> list[float]:
        if value is None:
            return []
        if isinstance(value, list):
            return [float(item) for item in value]
        text = str(value).strip().strip("[]")
        return [float(item) for item in text.split(",") if item] if text else []

    @staticmethod
    def _json(value: dict[str, Any]) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _decode_json(value: Any) -> dict[str, Any]:
        return value if isinstance(value, dict) else json.loads(str(value))

    @staticmethod
    def _is_unique_violation(exc: Exception) -> bool:
        """Avoid exposing psycopg classes through the repository interface."""
        return getattr(exc, "sqlstate", None) == "23505" or getattr(exc.__cause__, "sqlstate", None) == "23505"

    @staticmethod
    def _fingerprint(query: list[float], filters: dict[str, Any], model: str | None) -> str:
        from .store import AssetStore

        return AssetStore._query_fingerprint(query, filters, model)

    @staticmethod
    def _encode_cursor(payload: dict[str, Any]) -> str:
        from .store import AssetStore

        return AssetStore._encode_cursor(payload)

    @staticmethod
    def _decode_cursor(value: str) -> dict[str, Any]:
        from .store import AssetStore

        parsed = AssetStore._decode_cursor_payload(value)
        required = {"v", "fingerprint", "snapshot_generation", "last_distance", "last_asset_id"}
        if parsed.get("v") != 2 or not required.issubset(parsed):
            raise CursorError("invalid PostgreSQL cursor")
        return parsed
