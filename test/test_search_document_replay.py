#!/usr/bin/env python3
"""End-to-end test for the durable SearchDocument replay bridge.

The Compute API is represented by a tiny in-process HTTP server while the
SearchNode is the real C++ binary.  This keeps the test independent of
PostgreSQL/Redis while exercising the actual HTTP projection path.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


DOCUMENTS = [
    {
        "doc_id": "asset-1",
        "object_id": "A1",
        "object_version": 3,
        "tenant_id": "tenant-a",
        "media_type": "image/jpeg",
        "metadata": {"camera_model": "D610"},
        "thumbnail_ref": {"object_id": "T1", "object_version": 1},
        "embedding": {"model_id": "hash-v1", "vector": [1.0, 0.0]},
        "processor_version": "image-index-v1",
        "index_generation": 7,
        "state": "ready",
    },
    {
        "doc_id": "asset-2",
        "object_id": "A2",
        "object_version": 1,
        "tenant_id": "tenant-a",
        "media_type": "image/png",
        "metadata": {"width": "128"},
        "thumbnail_ref": None,
        "embedding": None,
        "processor_version": "image-index-v1",
        "index_generation": 7,
        "state": "ready",
    },
]


class ComputeHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        if self.path.split("?", 1)[0] != "/compute/search-documents":
            self.send_error(404)
            return
        body = json.dumps({"documents": DOCUMENTS}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args: object) -> None:
        return


def get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_health(url: str) -> dict:
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            return get_json(url)
        except OSError:
            time.sleep(0.05)
    raise AssertionError("SearchNode did not become healthy")


def run_replay(repo: Path, api_url: str, search_url: str) -> dict:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo / "compute-plane-v1")
    env["NO_PROXY"] = "127.0.0.1,localhost"
    env["no_proxy"] = "127.0.0.1,localhost"
    command = [
        sys.executable,
        str(repo / "compute-plane-v1/tools/replay_search_documents.py"),
        "--compute-api",
        api_url,
        "--search-node",
        search_url,
        "--limit",
        "10",
        "--flush",
    ]
    result = subprocess.run(command, cwd=repo, env=env, check=True,
                            capture_output=True, text=True)
    return json.loads(result.stdout)


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: test_search_document_replay.py SEARCH_NODE_BINARY")
    # The development environment may export a loopback HTTP proxy.  The
    # service under test is intentionally local, so all urllib calls in this
    # process must bypass that proxy as well as the replay subprocess.
    os.environ["NO_PROXY"] = "127.0.0.1,localhost"
    os.environ["no_proxy"] = "127.0.0.1,localhost"
    repo = Path(__file__).resolve().parents[1]
    binary = Path(sys.argv[1]).resolve()

    compute_server = ThreadingHTTPServer(("127.0.0.1", 0), ComputeHandler)
    compute_thread = threading.Thread(target=compute_server.serve_forever, daemon=True)
    compute_thread.start()

    with tempfile.TemporaryDirectory(prefix="minikv-search-replay-") as index_dir:
        search_port = 0
        # Ask the kernel for an unused port, then close it before starting the
        # child.  The existing HTTP E2E uses the same short-lived test pattern.
        import socket
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            search_port = probe.getsockname()[1]
        process = subprocess.Popen(
            [str(binary), str(search_port), index_dir, "2"],
            cwd=repo,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, "NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost"},
        )
        try:
            search_url = f"http://127.0.0.1:{search_port}"
            health = wait_health(search_url + "/healthz")
            assert health["documents"] == 0, health
            api_url = f"http://127.0.0.1:{compute_server.server_port}"

            first = run_replay(repo, api_url, search_url)
            assert first["documents"] == 2, first
            assert first["results"]["applied"] == 2, first
            assert first["flush"]["status"] == "flushed", first

            second = run_replay(repo, api_url, search_url)
            assert second["results"]["idempotent"] == 2, second
            health = get_json(search_url + "/healthz")
            assert health["documents"] == 2, health
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            compute_server.shutdown()
            compute_server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
