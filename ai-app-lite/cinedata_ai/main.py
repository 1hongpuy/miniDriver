from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import time
from pathlib import Path

from .config import Settings, ensure_parent
from .dataset import enqueue_labeled_directory
from .embeddings import create_provider
from .evaluation import evaluate_retrieval, load_cases
from .repository import create_store
from .store import AssetStore
from .stream_worker import build_ingestor
from .worker import build_worker


def main() -> None:
    parser = argparse.ArgumentParser(description="MiniDrive AI-App-Lite")
    subparsers = parser.add_subparsers(dest="command", required=True)
    api = subparsers.add_parser("api", help="start the HTTP API")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=18290)
    subparsers.add_parser("worker", help="run the local asset worker")
    subparsers.add_parser("stream-worker", help="consume MiniDrive FILE_UPLOAD_COMMITTED events")
    evaluate = subparsers.add_parser("evaluate", help="run fixed retrieval cases against the local index")
    evaluate.add_argument("--cases", required=True, help="JSON array of query/expected_asset_ids cases")
    evaluate.add_argument("--top-k", type=int, default=5)
    index_directory = subparsers.add_parser("index-directory", help="enqueue a folder-labelled raster image corpus")
    index_directory.add_argument("--root", required=True, help="dataset root; each first-level directory is a label")
    index_directory.add_argument("--cases-output", required=True, help="write generated evaluation cases JSON here")
    drain = subparsers.add_parser("drain-worker", help="process pending local jobs once and exit")
    drain.add_argument("--max-batches", type=int, default=0, help="0 drains all pending jobs")
    benchmark = subparsers.add_parser(
        "benchmark-query", help="measure concurrent vector-index query execution against the local store"
    )
    benchmark.add_argument("--query", required=True)
    benchmark.add_argument("--requests", type=int, default=100)
    benchmark.add_argument("--concurrency", type=int, default=4)
    benchmark.add_argument("--page-size", type=int, default=10)
    benchmark.add_argument("--filters", default="{}", help="JSON metadata filter object")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings()
    ensure_parent(settings.db_path)
    if args.command == "api":
        from .server import serve

        serve(settings, args.host, args.port)
    elif args.command == "worker":
        build_worker(settings).run_forever(settings.worker_poll_seconds)
    elif args.command == "stream-worker":
        build_ingestor(settings).run_forever()
    elif args.command == "evaluate":
        provider = create_provider(settings.embedding_provider, settings.embedding_dim,
                                   settings.openclip_model, settings.openclip_pretrained)
        result = evaluate_retrieval(create_store(settings), provider,
                                    load_cases(Path(args.cases)), args.top_k)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif args.command == "index-directory":
        cases_path = Path(args.cases_output).expanduser()
        queued, cases = enqueue_labeled_directory(create_store(settings), Path(args.root))
        cases_path.parent.mkdir(parents=True, exist_ok=True)
        cases_path.write_text(json.dumps(cases, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"queued": queued, "cases_output": str(cases_path), "case_count": len(cases)},
                         ensure_ascii=False))
    elif args.command == "benchmark-query":
        try:
            filters = json.loads(args.filters)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"--filters must be a JSON object: {exc}") from exc
        if not isinstance(filters, dict):
            raise SystemExit("--filters must be a JSON object")
        request_count = max(1, args.requests)
        concurrency = max(1, min(args.concurrency, request_count))
        provider = create_provider(settings.embedding_provider, settings.embedding_dim,
                                   settings.openclip_model, settings.openclip_pretrained)
        embedded_at = time.perf_counter()
        vector = provider.embed_text(args.query)
        embedding_once_ms = (time.perf_counter() - embedded_at) * 1000
        store = create_store(settings)

        def execute_once() -> tuple[float, int]:
            started = time.perf_counter()
            page = store.search_page(vector, args.page_size, filters, provider.model_id)
            return (time.perf_counter() - started) * 1000, len(page.results)

        started_at = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            samples = list(executor.map(lambda _index: execute_once(), range(request_count)))
        elapsed_seconds = max(0.000001, time.perf_counter() - started_at)
        latencies = sorted(sample[0] for sample in samples)

        def percentile(fraction: float) -> float:
            return latencies[min(len(latencies) - 1, int((len(latencies) - 1) * fraction))]

        print(json.dumps({
            "query": args.query,
            "embedding_model": provider.model_id,
            "filters": filters,
            "requests": request_count,
            "concurrency": concurrency,
            "page_size": args.page_size,
            "embedding_once_ms": round(embedding_once_ms, 3),
            "query_execution_ms": {
                "p50": round(percentile(0.50), 3),
                "p95": round(percentile(0.95), 3),
                "max": round(latencies[-1], 3),
            },
            "wall_seconds": round(elapsed_seconds, 6),
            "qps": round(request_count / elapsed_seconds, 3),
            "result_count": samples[0][1] if samples else 0,
            "scope": "local AssetStore vector scan; excludes HTTP and repeated text embedding",
        }, ensure_ascii=False, indent=2))
    else:
        worker = build_worker(settings)
        batches = 0
        jobs = 0
        while args.max_batches <= 0 or batches < args.max_batches:
            completed = worker.process_batch()
            if completed == 0:
                break
            jobs += completed
            batches += 1
        print(json.dumps({"processed_jobs": jobs, "batches": batches}, ensure_ascii=False))


if __name__ == "__main__":
    main()
