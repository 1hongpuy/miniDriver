from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from cinedata_ai.config import Settings
from cinedata_ai.embeddings import EmbeddingProvider
from cinedata_ai.server import ApiHandler, QueryMetrics
from cinedata_ai.store import AssetStore


class GovernedQueryProvider(EmbeddingProvider):
    name = "governed-query"
    model_id = "governed-query@v1"

    def embed_image(self, path: Path, text_hint: str = "") -> list[float]:
        del path, text_hint
        return [1.0, 0.0]

    def embed_text(self, text: str) -> list[float]:
        del text
        return [1.0, 0.0]


class PhaseEFGovernedAgentTest(unittest.TestCase):
    def _store(self, root: Path) -> AssetStore:
        store = AssetStore(root / "ai.sqlite3")
        store.upsert_asset(
            "asset-sunset", "/sunsets/sunset.jpg", "minidrive://sunset",
            {"semantic_tags": ["日落"], "object_version": 1}, [1.0, 0.0],
            "test", GovernedQueryProvider.model_id,
        )
        store.upsert_asset(
            "asset-buddha", "/buddha/buddha.jpg", "minidrive://buddha",
            {"semantic_tags": ["佛像"], "object_version": 3}, [0.8, 0.6],
            "test", GovernedQueryProvider.model_id,
        )
        return store

    def test_dataset_members_snapshot_object_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(Path(directory))
            dataset = store.create_dataset("摄影训练集", "alice", "验证素材版本快照")
            first = store.add_dataset_members(dataset["dataset_id"], ["asset-sunset"])
            self.assertEqual(first[0]["object_version"], 1)

            # Re-indexing the asset represents a later object version.  The
            # already created dataset remains a reference to v1, not a mutable
            # pointer to the current Asset row.
            store.upsert_asset(
                "asset-sunset", "/sunsets/sunset.jpg", "minidrive://sunset-v2",
                {"semantic_tags": ["日落"], "object_version": 2}, [1.0, 0.0],
                "test", GovernedQueryProvider.model_id,
            )
            current = store.get_asset("asset-sunset")
            self.assertEqual(current["metadata"]["object_version"], 2)
            snapshot = store.dataset(dataset["dataset_id"])
            self.assertEqual(snapshot["members"][0]["object_version"], 1)

            # Selecting the revised asset deliberately creates a new explicit
            # membership rather than silently changing the prior record.
            second = store.add_dataset_members(dataset["dataset_id"], ["asset-sunset"])
            self.assertEqual(second[0]["object_version"], 2)
            versions = [member["object_version"] for member in store.dataset(dataset["dataset_id"])["members"]]
            self.assertEqual(versions, [1, 2])

    def test_agent_search_is_read_only_and_records_versioned_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = self._store(root)
            handler = object.__new__(ApiHandler)
            handler.server = SimpleNamespace(
                store=store, provider=GovernedQueryProvider(), query_metrics=QueryMetrics(),
                settings=Settings(db_path=root / "ai.sqlite3"),
            )
            sent: dict[str, object] = {}
            handler._send = lambda status, payload: sent.update(status=status, payload=payload)

            before_assets = store.all_assets()
            handler._agent_search({"caller": "alice", "query": "日落", "page_size": 2})
            response = sent["payload"]
            self.assertEqual(response["mode"], "read-only-retrieval")
            self.assertTrue(response["run_id"])
            self.assertEqual(len(response["evidence"]), 2)
            run = store.agent_run(response["run_id"])
            self.assertEqual(run["caller"], "alice")
            self.assertEqual(run["query"], "日落")
            self.assertEqual(run["evidence"][0]["object_version"], 1)

            # F0 has no mutation tool: agent invocation only creates an audit
            # record, leaving the object/index contents untouched.
            self.assertEqual(store.all_assets(), before_assets)


if __name__ == "__main__":
    unittest.main()
