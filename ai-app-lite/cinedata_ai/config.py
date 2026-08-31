from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    # sqlite is a dependency-free fallback. postgres is the Phase D default for
    # a multi-process AI service and requires PostgreSQL with pgvector.
    storage_backend: str = os.getenv("AI_STORAGE_BACKEND", "sqlite").lower()
    db_path: Path = Path(os.getenv("AI_APP_DB", "./ai-app-lite/data/ai.sqlite3"))
    postgres_dsn: str = os.getenv(
        "AI_POSTGRES_DSN", "postgresql://cinedata:cinedata@127.0.0.1:54329/cinedata"
    )
    embedding_provider: str = os.getenv("AI_EMBEDDING_PROVIDER", "hash")
    embedding_dim: int = int(os.getenv("AI_EMBEDDING_DIM", "256"))
    openclip_model: str = os.getenv("AI_OPENCLIP_MODEL", "ViT-B-32-quickgelu")
    openclip_pretrained: str = os.getenv("AI_OPENCLIP_PRETRAINED", "openai")
    semantic_tag_top_k: int = int(os.getenv("AI_SEMANTIC_TAG_TOP_K", "3"))
    semantic_tag_min_score: float = float(os.getenv("AI_SEMANTIC_TAG_MIN_SCORE", "0.20"))
    llm_base_url: str = os.getenv("AI_LLM_BASE_URL", "").rstrip("/")
    llm_api_key: str = os.getenv("AI_LLM_API_KEY", "")
    llm_model: str = os.getenv("AI_LLM_MODEL", "")
    worker_poll_seconds: float = float(os.getenv("AI_WORKER_POLL_SECONDS", "0.5"))
    worker_batch_size: int = int(os.getenv("AI_WORKER_BATCH_SIZE", "4"))
    # Development default for the separately served MiniDrive web UI. Set a
    # concrete trusted origin before exposing this service beyond local use.
    cors_allow_origin: str = os.getenv("AI_CORS_ALLOW_ORIGIN", "*")
    redis_address: str = os.getenv("AI_REDIS_ADDRESS", "127.0.0.1")
    redis_port: int = int(os.getenv("AI_REDIS_PORT", "6379"))
    redis_file_event_stream: str = os.getenv("AI_REDIS_FILE_EVENT_STREAM", "minidrive:file-events")
    redis_consumer_group: str = os.getenv("AI_REDIS_CONSUMER_GROUP", "cinedata-indexers")
    redis_consumer_name: str = os.getenv("AI_REDIS_CONSUMER_NAME", "ai-worker-0")
    redis_block_ms: int = int(os.getenv("AI_REDIS_BLOCK_MS", "500"))
    redis_reclaim_idle_ms: int = int(os.getenv("AI_REDIS_RECLAIM_IDLE_MS", "300000"))
    minidrive_gateway_url: str = os.getenv("AI_MINIDRIVE_GATEWAY_URL", "").rstrip("/")
    object_temp_dir: Path = Path(os.getenv("AI_OBJECT_TEMP_DIR", "./ai-app-lite/data/object-tmp"))
    max_object_bytes: int = int(os.getenv("AI_MAX_OBJECT_BYTES", str(512 * 1024 * 1024)))


def ensure_parent(path: Path) -> None:
    path.expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
