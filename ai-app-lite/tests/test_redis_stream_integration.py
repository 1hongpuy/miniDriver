from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from cinedata_ai.redis_stream import _RespConnection
from cinedata_ai.redis_stream import RedisStreamConsumer
from cinedata_ai.store import AssetStore
from cinedata_ai.stream_worker import FileEventIngestor


@unittest.skipUnless(os.getenv("AI_REDIS_TEST_PORT"), "requires a test Redis server")
class RedisStreamIntegrationTest(unittest.TestCase):
    def test_consumer_persists_then_acknowledges_event(self) -> None:
        port = int(os.environ["AI_REDIS_TEST_PORT"])
        stream = os.environ.get("AI_REDIS_TEST_STREAM", "minidrive:file-events:python-test")
        connection = _RespConnection("127.0.0.1", port, 5)
        try:
            connection.execute([
                "XADD", stream, "*",
                "eventId", "event-python-1",
                "eventType", "FILE_UPLOAD_COMMITTED",
                "objectId", "object-python-1",
                "objectKey", "/travel/temple.jpg",
                "fileHash", "manifest-python-hash",
                "fileSize", "1024",
                "objectVersion", "1",
                "metadataVersion", "1",
                "occurredAt", "100",
            ])
        finally:
            connection.close()

        with tempfile.TemporaryDirectory() as directory:
            store = AssetStore(Path(directory) / "ai.sqlite3")
            consumer = RedisStreamConsumer(
                "127.0.0.1", port, stream, "cinedata-test", "consumer-1", block_ms=10, reclaim_idle_ms=10
            )
            self.assertTrue(FileEventIngestor(store, consumer).process_once())
            targets = store.all_assets()
            self.assertEqual(targets, [])
            with store._connect() as db:  # test the durable Job boundary, not vector output
                row = db.execute("SELECT job_id FROM index_targets WHERE object_id='object-python-1'").fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(store.job(row["job_id"])["status"], "pending")


if __name__ == "__main__":
    unittest.main()
