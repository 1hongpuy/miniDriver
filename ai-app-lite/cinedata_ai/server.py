from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .config import Settings
from .embeddings import EmbeddingProvider
from .repository import create_store
from .store import AssetStore

LOG = logging.getLogger("cinedata_ai.api")


class QueryMetrics:
    """Bounded, in-process query observations for the Phase D Lite service.

    This is deliberately not a metrics backend.  It makes the latency and
    result-count evidence visible in a one-process demo while retaining a
    simple `/ai/metrics` contract that Prometheus/OpenTelemetry can replace.
    """

    def __init__(self, max_samples: int = 512):
        self._max_samples = max(16, max_samples)
        self._samples: list[float] = []
        self._result_counts: list[int] = []
        self._requests = 0
        self._errors = 0
        self._lock = threading.Lock()

    def record(self, elapsed_ms: float, result_count: int = 0, error: bool = False) -> None:
        with self._lock:
            self._requests += 1
            self._errors += int(error)
            self._samples.append(max(0.0, elapsed_ms))
            self._result_counts.append(max(0, result_count))
            del self._samples[:-self._max_samples]
            del self._result_counts[:-self._max_samples]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            samples = sorted(self._samples)
            counts = list(self._result_counts)
            requests = self._requests
            errors = self._errors
        return {
            "requests": requests,
            "errors": errors,
            "sample_window": len(samples),
            "latency_ms": {
                "p50": self._percentile(samples, 0.50),
                "p95": self._percentile(samples, 0.95),
                "max": samples[-1] if samples else None,
            },
            "result_count": {
                "mean": (sum(counts) / len(counts)) if counts else None,
                "max": max(counts) if counts else None,
            },
        }

    @staticmethod
    def _percentile(samples: list[float], fraction: float) -> float | None:
        if not samples:
            return None
        index = min(len(samples) - 1, max(0, int((len(samples) - 1) * fraction)))
        return samples[index]


class ApiHandler(BaseHTTPRequestHandler):
    server: "AiHttpServer"

    def do_OPTIONS(self) -> None:  # noqa: N802 - stdlib HTTPServer API
        self.send_response(HTTPStatus.NO_CONTENT)
        self._send_cors_headers()
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Max-Age", "600")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802 - stdlib HTTPServer API
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path
        if path == "/healthz":
            self._send(HTTPStatus.OK, {
                "status": "ok", "service": "ai-app-lite", "index": self.server.store.index_stats(),
            })
            return
        if path == "/ai/metrics":
            self._send(HTTPStatus.OK, {
                "query": self.server.query_metrics.snapshot(),
                "index": self.server.store.index_stats(),
            })
            return
        if path == "/ai/datasets":
            owner = urllib.parse.parse_qs(parsed.query).get("owner", [None])[0]
            self._send(HTTPStatus.OK, {"datasets": self.server.store.list_datasets(owner)})
            return
        if path.startswith("/ai/datasets/"):
            dataset_id = path.rsplit("/", 1)[-1]
            dataset = self.server.store.dataset(dataset_id)
            self._send(HTTPStatus.OK if dataset else HTTPStatus.NOT_FOUND,
                       dataset or {"error": "dataset not found"})
            return
        if path.startswith("/ai/agent/runs/"):
            run_id = path.rsplit("/", 1)[-1]
            run = self.server.store.agent_run(run_id)
            self._send(HTTPStatus.OK if run else HTTPStatus.NOT_FOUND, run or {"error": "agent run not found"})
            return
        if path.startswith("/ai/jobs/"):
            job_id = path.rsplit("/", 1)[-1]
            job = self.server.store.job(job_id)
            self._send(HTTPStatus.OK if job else HTTPStatus.NOT_FOUND, job or {"error": "job not found"})
            return
        self._send(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802 - stdlib HTTPServer API
        try:
            body = self._read_json()
            path = urllib.parse.urlsplit(self.path).path
            if path == "/ai/assets":
                self._enqueue_asset(body)
            elif path == "/ai/search":
                self._search(body)
            elif path == "/ai/datasets":
                self._create_dataset(body)
            elif path.startswith("/ai/datasets/") and path.endswith("/members"):
                self._add_dataset_members(path, body)
            elif path == "/ai/agent/search":
                self._agent_search(body)
            elif path == "/ai/caption":
                self._caption(body)
            else:
                self._send(HTTPStatus.NOT_FOUND, {"error": "not found"})
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            self._send(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except Exception as exc:  # keep the demo API alive on one bad request
            LOG.exception("request failed")
            self._send(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})

    def _enqueue_asset(self, body: dict[str, Any]) -> None:
        for key in ("asset_id", "local_path"):
            if not body.get(key):
                raise ValueError(f"missing {key}")
        job_id = self.server.store.enqueue_asset(body)
        self._send(HTTPStatus.ACCEPTED, {"job_id": job_id, "status": "pending"})

    def _search(self, body: dict[str, Any]) -> None:
        self._send(HTTPStatus.OK, self._search_response(body))

    def _search_response(self, body: dict[str, Any]) -> dict[str, Any]:
        query = str(body.get("query", "")).strip()
        if not query:
            raise ValueError("query is required")
        filters = body.get("filters") or {}
        if not isinstance(filters, dict):
            raise ValueError("filters must be an object")
        page_size = int(body.get("page_size", body.get("top_k", 10)))
        cursor = body.get("cursor")
        if cursor is not None and not isinstance(cursor, str):
            raise ValueError("cursor must be a string")
        started_at = time.perf_counter()
        try:
            vector = self.server.provider.embed_text(query)
            page = self.server.store.search_page(
                vector, page_size, filters, self.server.provider.model_id, cursor,
            )
        except Exception:
            self.server.query_metrics.record((time.perf_counter() - started_at) * 1000, error=True)
            raise
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        self.server.query_metrics.record(elapsed_ms, len(page.results))
        results = [self._public_search_result(result) for result in page.results]
        return {
            "query": query,
            "embedding_model": self.server.provider.model_id,
            "filters": filters,
            "results": results,
            "page": {
                "page_size": page_size,
                "next_cursor": page.next_cursor,
                "total_candidates": page.total_candidates,
                "stable_sort": "score_desc,asset_id_asc",
                "snapshot_updated_at": page.snapshot_updated_at,
            },
            "query_latency_ms": round(elapsed_ms, 3),
        }

    def _create_dataset(self, body: dict[str, Any]) -> None:
        dataset = self.server.store.create_dataset(
            str(body.get("name", "")), str(body.get("owner", "")), str(body.get("purpose", "")),
        )
        self._send(HTTPStatus.CREATED, dataset)

    def _add_dataset_members(self, path: str, body: dict[str, Any]) -> None:
        dataset_id = path.removeprefix("/ai/datasets/").removesuffix("/members").rstrip("/")
        if not dataset_id:
            raise ValueError("dataset_id is required")
        asset_ids = body.get("asset_ids")
        if not isinstance(asset_ids, list):
            raise ValueError("asset_ids must be an array")
        members = self.server.store.add_dataset_members(dataset_id, asset_ids, str(body.get("role", "train")))
        self._send(HTTPStatus.OK, {"dataset_id": dataset_id, "members": members})

    def _agent_search(self, body: dict[str, Any]) -> None:
        """Read-only retrieval Agent: it can search and cite, never mutate data."""
        caller = str(body.get("caller", "")).strip()
        if not caller:
            raise ValueError("caller is required")
        payload = self._search_response(body)
        run = self.server.store.record_agent_search(
            caller, payload["query"], payload["filters"], payload["embedding_model"], payload["results"],
        )
        citations = [f"{item['asset_id']}@v{item['object_version']}" for item in payload["results"]]
        answer = (
            f"检索到 {len(citations)} 个可用素材。以下结果按语义相关度排序，"
            f"可作为只读证据：{', '.join(citations) if citations else '无'}。"
        )
        self._send(HTTPStatus.OK, {
            "mode": "read-only-retrieval",
            "run_id": run["run_id"],
            "answer": answer,
            "evidence": run["evidence"],
            "search": payload,
        })

    @staticmethod
    def _public_search_result(result: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(result.get("metadata") or {})
        return {
            "asset_id": result["asset_id"],
            "object_key": result["object_key"],
            "object_version": metadata.get("object_version", 1),
            "status": result.get("status"),
            "updated_at": result.get("updated_at"),
            "score": result.get("score", 0.0),
            "metadata": metadata,
            "object_route": {"object_id": result["asset_id"], "object_key": result["object_key"]},
            # The Lite pipeline has no derivative worker yet.  Returning an
            # explicit state avoids pretending every cross-directory result
            # owns a thumbnail URL.
            "thumbnail": {"state": "unavailable", "url": None},
        }

    def _caption(self, body: dict[str, Any]) -> None:
        asset_ids = body.get("asset_ids") or []
        if not asset_ids:
            raise ValueError("asset_ids is required")
        assets = [self.server.store.get_asset(str(asset_id)) for asset_id in asset_ids]
        assets = [asset for asset in assets if asset is not None]
        instruction = str(body.get("instruction", "写一段简洁的摄影作品说明"))
        if self.server.settings.llm_base_url and self.server.settings.llm_model:
            caption = self._call_llm(instruction, assets)
            mode = "llm"
        else:
            names = [asset["object_key"] for asset in assets]
            cameras = [asset["metadata"].get("camera_model") for asset in assets]
            camera = next((value for value in cameras if value), "未记录相机")
            caption = f"{instruction}：共选取 {len(names)} 个素材（{camera}），包括：{', '.join(names)}。"
            mode = "template-fallback"
        self._send(HTTPStatus.OK, {"mode": mode, "caption": caption, "asset_ids": asset_ids})

    def _call_llm(self, instruction: str, assets: list[dict[str, Any]]) -> str:
        prompt = {
            "instruction": instruction,
            "assets": [
                {"object_key": asset["object_key"], "metadata": asset["metadata"]}
                for asset in assets
            ],
        }
        request_body = json.dumps(
            {
                "model": self.server.settings.llm_model,
                "messages": [
                    {"role": "system", "content": "你是摄影素材整理助手。"},
                    {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
                ],
                "temperature": 0.4,
            },
            ensure_ascii=False,
        ).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.server.settings.llm_api_key:
            headers["Authorization"] = f"Bearer {self.server.settings.llm_api_key}"
        request = urllib.request.Request(
            self.server.settings.llm_base_url + "/chat/completions",
            data=request_body,
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return str(payload["choices"][0]["message"]["content"])

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > 2 * 1024 * 1024:
            raise ValueError("JSON body must be between 1 byte and 2 MiB")
        value = json.loads(self.rfile.read(length).decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("JSON body must be an object")
        return value

    def _send(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self._send_cors_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", self.server.settings.cors_allow_origin)

    def log_message(self, format: str, *args: Any) -> None:
        LOG.info("%s - %s", self.address_string(), format % args)


class AiHttpServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], store: AssetStore, provider: EmbeddingProvider, settings: Settings):
        super().__init__(address, ApiHandler)
        self.store = store
        self.provider = provider
        self.settings = settings
        self.query_metrics = QueryMetrics()


def serve(settings: Settings, host: str = "127.0.0.1", port: int = 18290) -> None:
    from .embeddings import create_provider

    store = create_store(settings)
    provider = create_provider(
        settings.embedding_provider, settings.embedding_dim,
        settings.openclip_model, settings.openclip_pretrained,
    )
    server = AiHttpServer((host, port), store, provider, settings)
    LOG.info("AI-App-Lite listening on %s:%s provider=%s", host, port, provider.name)
    try:
        server.serve_forever()
    finally:
        server.server_close()
