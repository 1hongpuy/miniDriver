from __future__ import annotations

from contextlib import contextmanager
import tempfile
import unittest
from pathlib import Path

from cinedata_ai.embeddings import HashEmbeddingProvider
from cinedata_ai.store import AssetStore
from cinedata_ai.worker import AssetWorker


class FakeMiniDriveReader:
    def __init__(self, path: Path):
        self.path = path
        self.received: dict[str, object] | None = None

    @contextmanager
    def materialize(self, event: dict[str, object]):
        self.received = event
        yield self.path


class StreamEventStoreTest(unittest.TestCase):
    def test_duplicate_event_and_target_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = AssetStore(Path(directory) / "ai.sqlite3")
            event = {
                "eventId": "event-1",
                "eventType": "FILE_UPLOAD_COMMITTED",
                "objectId": "object-1",
                "objectKey": "/travel/temple.jpg",
                "fileHash": "manifest-hash",
                "fileSize": "1024",
                "objectVersion": "1",
                "metadataVersion": "1",
            }
            job_id = store.enqueue_committed_object(event)
            self.assertEqual(store.enqueue_committed_object(event), job_id)

            retry = dict(event)
            retry["eventId"] = "event-2"
            self.assertEqual(store.enqueue_committed_object(retry), job_id)
            job = store.job(job_id)
            self.assertEqual(job["status"], "pending")
            self.assertEqual(job["payload"]["source"], "minidrive")
            self.assertEqual(job["payload"]["asset_id"], "object-1")

    def test_committed_object_job_uses_reader_and_keeps_logical_locator(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "temple-sunset.jpg"
            source.write_bytes(b"committed-object-bytes")
            store = AssetStore(root / "ai.sqlite3")
            job_id = store.enqueue_committed_object({
                "eventId": "event-1",
                "eventType": "FILE_UPLOAD_COMMITTED",
                "objectId": "object-1",
                "objectKey": "/travel/temple-sunset.jpg",
                "fileHash": "manifest-hash",
                "fileSize": str(source.stat().st_size),
                "objectVersion": "1",
                "metadataVersion": "1",
            })
            reader = FakeMiniDriveReader(source)
            worker = AssetWorker(store, HashEmbeddingProvider(32), reader)  # type: ignore[arg-type]
            self.assertTrue(worker.process_once())
            self.assertEqual(store.job(job_id)["status"], "done")
            self.assertEqual(reader.received["asset_id"], "object-1")  # type: ignore[index]
            asset = store.get_asset("object-1")
            self.assertEqual(asset["local_path"], "minidrive://object-1")
            self.assertEqual(asset["metadata"]["object_version"], 1)


if __name__ == "__main__":
    unittest.main()
