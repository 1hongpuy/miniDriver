from __future__ import annotations

import os
import unittest
import uuid

from cinedata_ai.postgres_store import PostgresAssetStore


POSTGRES_DSN = os.getenv("AI_TEST_POSTGRES_DSN", "")


@unittest.skipUnless(POSTGRES_DSN, "set AI_TEST_POSTGRES_DSN to run PostgreSQL + pgvector integration")
class PostgresStoreIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = PostgresAssetStore(POSTGRES_DSN)
        self.prefix = "pg-test-" + uuid.uuid4().hex
        self.model = self.prefix + "@v1"

    def test_vector_paging_filter_and_skip_locked_job_claim(self) -> None:
        self.store.upsert_asset(
            self.prefix + "-bird-a", "/birds/a.jpg", "minidrive://a",
            {"semantic_tags": ["鸟类"], "quality_score": 0.91}, [1.0, 0.0], "test", self.model,
        )
        self.store.upsert_asset(
            self.prefix + "-bird-b", "/birds/b.jpg", "minidrive://b",
            {"semantic_tags": ["鸟类"], "quality_score": 0.82}, [1.0, 0.0], "test", self.model,
        )
        self.store.upsert_asset(
            self.prefix + "-mountain", "/mountains/c.jpg", "minidrive://c",
            {"semantic_tags": ["山景"], "quality_score": 0.73}, [0.8, 0.6], "test", self.model,
        )
        first = self.store.search_page([1.0, 0.0], page_size=2, embedding_model=self.model)
        self.assertEqual(len(first.results), 2)
        self.assertIsNotNone(first.next_cursor)
        second = self.store.search_page([1.0, 0.0], page_size=2, embedding_model=self.model,
                                        cursor=first.next_cursor)
        self.assertEqual([row["asset_id"] for row in second.results], [self.prefix + "-mountain"])
        birds = self.store.search_page([1.0, 0.0], page_size=5, embedding_model=self.model,
                                       metadata_filter={"path_prefix": "/birds", "semantic_tags": ["鸟类"],
                                                        "quality_min": 0.9})
        self.assertEqual([row["asset_id"] for row in birds.results], [self.prefix + "-bird-a"])

        job_id = self.store.enqueue_asset({"asset_id": self.prefix + "-queued", "local_path": "/tmp/x.jpg"})
        claim = self.store.claim_jobs(1)
        self.assertTrue(any(job.job_id == job_id for job in claim))
        self.store.finish_job(job_id)
        self.assertEqual(self.store.job(job_id)["status"], "done")

    def test_dataset_snapshot_and_agent_audit(self) -> None:
        asset_id = self.prefix + "-versioned"
        self.store.upsert_asset(
            asset_id, "/sunsets/a.jpg", "minidrive://v1",
            {"semantic_tags": ["日落"], "object_version": 1}, [1.0, 0.0], "test", self.model,
        )
        dataset = self.store.create_dataset("dataset-" + self.prefix, "pg-test-owner", "integration snapshot")
        added = self.store.add_dataset_members(dataset["dataset_id"], [asset_id])
        self.assertEqual(added[0]["object_version"], 1)

        self.store.upsert_asset(
            asset_id, "/sunsets/a.jpg", "minidrive://v2",
            {"semantic_tags": ["日落"], "object_version": 2}, [1.0, 0.0], "test", self.model,
        )
        self.assertEqual(self.store.dataset(dataset["dataset_id"])["members"][0]["object_version"], 1)
        search = self.store.search_page([1.0, 0.0], page_size=3, embedding_model=self.model)
        run = self.store.record_agent_search("pg-test-agent", "日落", {}, self.model, search.results)
        self.assertEqual(run["caller"], "pg-test-agent")
        self.assertTrue(run["evidence"])
        self.assertEqual(run["evidence"][0]["object_version"], 2)
