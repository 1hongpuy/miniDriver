from __future__ import annotations

import logging
import time

from .config import Settings
from .repository import create_store
from .redis_stream import RedisStreamConsumer
from .store import AssetStore

LOG = logging.getLogger("cinedata_ai.stream_worker")


class FileEventIngestor:
    """Durably turns FILE_UPLOAD_COMMITTED messages into local index jobs.

    The Redis entry is acknowledged only after `enqueue_committed_object` has
    committed its de-duplication ledger and Job record to SQLite.
    """

    def __init__(self, store: AssetStore, consumer: RedisStreamConsumer):
        self.store = store
        self.consumer = consumer

    def process_once(self) -> bool:
        message = self.consumer.read_one()
        if message is None:
            return False
        event = message.fields
        if event.get("eventType") != "FILE_UPLOAD_COMMITTED":
            LOG.warning("ignoring unknown stream event id=%s type=%s", message.message_id, event.get("eventType"))
            self.consumer.acknowledge(message.message_id)
            return True
        job_id = self.store.enqueue_committed_object(event)
        self.consumer.acknowledge(message.message_id)
        LOG.info("accepted file event stream_id=%s event_id=%s job=%s", message.message_id, event.get("eventId"), job_id)
        return True

    def run_forever(self) -> None:
        LOG.info("file event ingestor started stream=%s group=%s consumer=%s",
                 self.consumer.stream, self.consumer.group, self.consumer.consumer)
        while True:
            try:
                self.process_once()
            except Exception:
                LOG.exception("file event ingestion failed; message remains pending for reclaim")
                time.sleep(1)


def build_ingestor(settings: Settings) -> FileEventIngestor:
    consumer = RedisStreamConsumer(
        settings.redis_address, settings.redis_port, settings.redis_file_event_stream,
        settings.redis_consumer_group, settings.redis_consumer_name,
        settings.redis_block_ms, settings.redis_reclaim_idle_ms,
    )
    return FileEventIngestor(create_store(settings), consumer)
