from __future__ import annotations

from typing import Any

from .config import Settings
from .store import AssetStore


def create_store(settings: Settings) -> Any:
    """Create the configured metadata/vector repository.

    Keep the call sites independent from SQLite/PostgreSQL details.  The
    repository APIs intentionally match while PostgreSQL owns the real Phase D
    production path and SQLite remains useful for offline demos and unit tests.
    """
    if settings.storage_backend == "sqlite":
        return AssetStore(settings.db_path)
    if settings.storage_backend == "postgres":
        from .postgres_store import PostgresAssetStore

        return PostgresAssetStore(settings.postgres_dsn)
    raise ValueError("AI_STORAGE_BACKEND must be 'sqlite' or 'postgres'")
