from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from cinedata_ai.config import Settings
from cinedata_ai.embeddings import EmbeddingProvider
from cinedata_ai.server import ApiHandler, QueryMetrics
from cinedata_ai.store import AssetStore, CursorError


class QueryStubProvider(EmbeddingProvider):
    name = "query-stub"
    model_id = "query-stub@v1"

    def embed_image(self, path: Path, text_hint: str = "") -> list[float]:
        del path, text_hint
        return [1.0, 0.0]

    def embed_text(self, text: str) -> list[float]:
        return [0.0, 1.0] if text == "other" else [1.0, 0.0]


class PhaseDQueryServiceTest(unittest.TestCase):
    def _store(self, root: Path) -> AssetStore:
        store = AssetStore(root / "ai.sqlite3")
        records = [
            ("asset-a", "/birds/a.jpg", [1.0, 0.0], {"semantic_tags": ["鸟类"], "quality_score": 0.9}),
            ("asset-b", "/birds/b.jpg", [1.0, 0.0], {"semantic_tags": ["鸟类"], "quality_score": 0.8}),
            ("asset-c", "/mountains/c.jpg", [0.8, 0.6], {"semantic_tags": ["山景"], "quality_score": 0.7}),
            ("asset-d", "/misc/d.jpg", [0.0, 1.0], {"semantic_tags": ["其他"], "quality_score": 0.2}),
        ]
        for asset_id, object_key, vector, metadata in records:
            store.upsert_asset(asset_id, object_key, "/private/local.jpg", metadata, vector,
                               "query-stub", "query-stub@v1")
        return store

    def test_keyset_pagination_filters_and_cursor_binding(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(Path(directory))
            first = store.search_page([1.0, 0.0], page_size=2, embedding_model="query-stub@v1")
            self.assertEqual([item["asset_id"] for item in first.results], ["asset-a", "asset-b"])
            self.assertEqual(first.total_candidates, 4)
            self.assertIsNotNone(first.next_cursor)

            second = store.search_page([1.0, 0.0], page_size=2, embedding_model="query-stub@v1",
                                       cursor=first.next_cursor)
            self.assertEqual([item["asset_id"] for item in second.results], ["asset-c", "asset-d"])
            self.assertIsNone(second.next_cursor)
            with self.assertRaises(CursorError):
                store.search_page([0.0, 1.0], page_size=2, embedding_model="query-stub@v1",
                                  cursor=first.next_cursor)

            birds = store.search_page([1.0, 0.0], page_size=10, embedding_model="query-stub@v1",
                                      metadata_filter={"path_prefix": "/birds", "semantic_tags": ["鸟类"],
                                                       "quality_min": 0.85})
            self.assertEqual([item["asset_id"] for item in birds.results], ["asset-a"])

            # A completed Worker write must invalidate the in-process read
            # snapshot; otherwise a newly indexed asset could remain invisible.
            store.upsert_asset("asset-e", "/birds/e.jpg", "/private/local.jpg", {"semantic_tags": ["鸟类"]},
                               [1.0, 0.0], "query-stub", "query-stub@v1")
            refreshed = store.search_page([1.0, 0.0], page_size=10, embedding_model="query-stub@v1")
            self.assertIn("asset-e", [item["asset_id"] for item in refreshed.results])

    def test_handler_contract_hides_local_paths_and_reports_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = self._store(root)
            handler = object.__new__(ApiHandler)
            handler.server = SimpleNamespace(
                store=store, provider=QueryStubProvider(), query_metrics=QueryMetrics(),
                settings=Settings(db_path=root / "ai.sqlite3"),
            )
            sent: dict[str, object] = {}
            handler._send = lambda status, payload: sent.update(status=status, payload=payload)
            handler._search({"query": "birds", "page_size": 2})
            result = sent["payload"]
            self.assertEqual(result["page"]["stable_sort"], "score_desc,asset_id_asc")
            self.assertEqual(len(result["results"]), 2)
            self.assertNotIn("local_path", result["results"][0])
            self.assertNotIn("embedding", result["results"][0])
            self.assertEqual(result["results"][0]["thumbnail"]["state"], "unavailable")
            metrics = handler.server.query_metrics.snapshot()
            self.assertEqual(metrics["requests"], 1)
            self.assertEqual(store.index_stats()["ready_assets"], 4)


if __name__ == "__main__":
    unittest.main()
