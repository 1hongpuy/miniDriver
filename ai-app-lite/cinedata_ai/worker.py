from __future__ import annotations

import hashlib
import logging
import time
from contextlib import ExitStack, nullcontext
from pathlib import Path
from typing import Any

from .config import Settings
from .embeddings import EmbeddingProvider, create_provider
from .exif import extract_metadata
from .media_analysis import analyze_media, media_kind
from .minidrive_reader import MiniDriveObjectReader
from .repository import create_store
from .store import AssetStore, Job

LOG = logging.getLogger("cinedata_ai.worker")


class AssetWorker:
    def __init__(self, store: AssetStore, provider: EmbeddingProvider,
                 minidrive_reader: MiniDriveObjectReader | None = None,
                 batch_size: int = 1, semantic_tag_top_k: int = 3,
                 semantic_tag_min_score: float = 0.20):
        self.store = store
        self.provider = provider
        self.minidrive_reader = minidrive_reader
        self.batch_size = max(1, batch_size)
        self.semantic_tag_top_k = max(1, semantic_tag_top_k)
        self.semantic_tag_min_score = semantic_tag_min_score

    def process_once(self) -> bool:
        return self.process_batch(1) > 0

    def process_batch(self, max_jobs: int | None = None) -> int:
        """Claim a bounded batch; each job remains independently observable.

        A malformed object must not poison the rest of the batch. The provider
        receives one batch only after all valid raster image inputs are prepared.
        """
        jobs = self.store.claim_jobs(max_jobs or self.batch_size)
        if not jobs:
            return 0
        prepared: list[tuple[Job, str, str, Path, dict[str, Any], dict[str, Any]]] = []
        with ExitStack() as stack:
            for job in jobs:
                try:
                    if job.kind != "index_asset":
                        raise ValueError(f"unsupported job type: {job.kind}")
                    asset_id, object_key, path = self._materialize(job, stack)
                    metadata = self._base_metadata(asset_id, path, job.payload)
                    self._ensure_supported_media(path)
                    prepared.append((job, asset_id, object_key, path, job.payload, metadata))
                except Exception as exc:
                    self.store.fail_job(job.job_id, str(exc))
                    LOG.info("asset job preparation failed job=%s error=%s", job.job_id, exc)

            if not prepared:
                return len(jobs)
            try:
                paths = [item[3] for item in prepared]
                hints = [self._text_hint(item[5]) for item in prepared]
                vectors = self.provider.embed_images(paths, hints)
                if len(vectors) != len(prepared):
                    raise RuntimeError("embedding provider returned an unexpected batch size")
                tags = self.provider.classify_images(
                    paths, self.semantic_tag_top_k, self.semantic_tag_min_score
                )
                if len(tags) != len(prepared):
                    raise RuntimeError("tag provider returned an unexpected batch size")
            except Exception as exc:
                for job, *_rest in prepared:
                    self.store.fail_job(job.job_id, f"batch inference failed: {exc}")
                LOG.exception("asset inference batch failed size=%s", len(prepared))
                return len(jobs)

            for (job, asset_id, object_key, path, payload, metadata), vector, predictions in zip(prepared, vectors, tags):
                try:
                    if predictions:
                        metadata["semantic_tags"] = [prediction.label for prediction in predictions]
                        metadata["semantic_tag_predictions"] = [
                            {"label": prediction.label, "score": round(prediction.score, 4),
                             "prompt": prediction.prompt, "source": self.provider.model_id}
                            for prediction in predictions
                        ]
                    elif self.provider.supports_semantic_tags:
                        metadata["semantic_tags"] = []
                    metadata["embedding_provider"] = self.provider.name
                    metadata["embedding_model"] = self.provider.model_id
                    self._persist_asset(asset_id, object_key, path, payload, metadata, vector)
                    self.store.finish_job(job.job_id)
                    LOG.info("indexed asset job=%s model=%s", job.job_id, self.provider.model_id)
                except Exception as exc:
                    self.store.fail_job(job.job_id, str(exc))
                    LOG.exception("asset job persistence failed job=%s", job.job_id)
        return len(jobs)

    def run_forever(self, poll_seconds: float = 0.5) -> None:
        LOG.info("asset worker started provider=%s", self.provider.name)
        while True:
            if self.process_batch() == 0:
                time.sleep(poll_seconds)

    def _materialize(self, job: Job, stack: ExitStack) -> tuple[str, str, Path]:
        payload = job.payload
        asset_id = str(payload["asset_id"])
        object_key = str(payload.get("object_key", asset_id))
        source = str(payload.get("source", "local"))
        if source == "minidrive":
            if self.minidrive_reader is None:
                raise RuntimeError("MiniDrive event job requires AI_MINIDRIVE_GATEWAY_URL")
            path_context = self.minidrive_reader.materialize(payload)
        elif source == "local":
            path_context = nullcontext(Path(str(payload["local_path"])).expanduser())
        else:
            raise ValueError(f"unsupported asset source: {source}")
        return asset_id, object_key, stack.enter_context(path_context)

    def _base_metadata(self, asset_id: str, path: Path, payload: dict[str, Any]) -> dict[str, Any]:
        metadata: dict[str, Any] = dict(payload.get("metadata") or {})
        metadata.update({key: value for key, value in extract_metadata(path).items() if key not in metadata})
        metadata.update({key: value for key, value in analyze_media(path).items() if key not in metadata})
        metadata["asset_id"] = asset_id
        for key in ("file_hash", "file_size", "object_version", "metadata_version", "event_id"):
            if key in payload:
                metadata[key] = payload[key]
        if path.is_file():
            metadata["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        return metadata

    def _ensure_supported_media(self, path: Path) -> None:
        if self.provider.supports_semantic_tags and media_kind(path) != "raster_image":
            raise ValueError("Phase C OpenCLIP indexing currently requires JPEG/PNG/WebP/TIFF raster input; "
                             "RAW/video require a preview/keyframe derivative worker")

    @staticmethod
    def _text_hint(metadata: dict[str, Any]) -> str:
        return " ".join(str(value) for value in metadata.values())

    def _persist_asset(self, asset_id: str, object_key: str, path: Path,
                       payload: dict[str, Any], metadata: dict[str, Any], vector: list[float]) -> None:
        stored_path = str(path)
        if payload.get("source") == "minidrive":
            # materialize() removes its temporary file after this job. Retain a
            # stable logical locator rather than a dead local filesystem path.
            stored_path = f"minidrive://{asset_id}"
        self.store.upsert_asset(asset_id, object_key, stored_path, metadata, vector,
                                self.provider.name, self.provider.model_id)


def build_worker(settings: Settings) -> AssetWorker:
    reader = None
    if settings.minidrive_gateway_url:
        reader = MiniDriveObjectReader(
            settings.minidrive_gateway_url, settings.object_temp_dir, settings.max_object_bytes
        )
    return AssetWorker(
        create_store(settings),
        create_provider(settings.embedding_provider, settings.embedding_dim,
                        settings.openclip_model, settings.openclip_pretrained),
        reader,
        settings.worker_batch_size,
        settings.semantic_tag_top_k,
        settings.semantic_tag_min_score,
    )
