from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cinedata_ai.embeddings import HashEmbeddingProvider
from cinedata_ai.store import AssetStore
from cinedata_ai.worker import AssetWorker


class PipelineTest(unittest.TestCase):
    def test_enqueue_worker_and_search(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sample = root / "temple-sunset.jpg"
            sample.write_bytes(b"demo-photo-bytes")
            store = AssetStore(root / "ai.sqlite3")
            job_id = store.enqueue_asset(
                {
                    "asset_id": "photo-1",
                    "object_key": "photos/temple-sunset.jpg",
                    "local_path": str(sample),
                    "metadata": {"scene": "temple sunset", "camera_model": "demo-camera"},
                }
            )
            worker = AssetWorker(store, HashEmbeddingProvider(64))
            self.assertTrue(worker.process_once())
            self.assertEqual(store.job(job_id)["status"], "done")
            asset = store.get_asset("photo-1")
            self.assertEqual(asset["metadata"]["scene"], "temple sunset")
            results = store.search(HashEmbeddingProvider(64).embed_text("temple sunset"), top_k=5)
            self.assertEqual(results[0]["asset_id"], "photo-1")

    def test_failed_job_is_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = AssetStore(Path(directory) / "ai.sqlite3")
            job_id = store.enqueue_asset(
                {"asset_id": "missing", "object_key": "missing.jpg", "local_path": "/not/exist.jpg"}
            )
            worker = AssetWorker(store, HashEmbeddingProvider(32))
            self.assertTrue(worker.process_once())
            self.assertEqual(store.job(job_id)["status"], "done")
            self.assertIsNotNone(store.get_asset("missing"))


if __name__ == "__main__":
    unittest.main()
