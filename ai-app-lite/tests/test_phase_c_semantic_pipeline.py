from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from PIL import Image

from cinedata_ai.embeddings import EmbeddingProvider, TagPrediction
from cinedata_ai.evaluation import RetrievalCase, evaluate_retrieval
from cinedata_ai.media_analysis import analyze_media, hash_distance
from cinedata_ai.store import AssetStore
from cinedata_ai.worker import AssetWorker


class SemanticStubProvider(EmbeddingProvider):
    name = "semantic-stub"
    model_id = "semantic-stub@v1"
    supports_semantic_tags = True

    def embed_image(self, path: Path, text_hint: str = "") -> list[float]:
        del text_hint
        return self.embed_images([path], [""])[0]

    def embed_images(self, paths: list[Path], text_hints: list[str]) -> list[list[float]]:
        del text_hints
        return [[1.0, 0.0] if "temple" in path.name else [0.0, 1.0] for path in paths]

    def embed_text(self, text: str) -> list[float]:
        return [1.0, 0.0] if "寺庙" in text or "temple" in text else [0.0, 1.0]

    def classify_images(self, paths: list[Path], top_k: int = 3,
                        min_score: float = 0.20) -> list[list[TagPrediction]]:
        del top_k, min_score
        return [[TagPrediction("寺庙", 0.93, "a temple or historic shrine")]
                if "temple" in path.name else
                [TagPrediction("鸟类", 0.91, "a bird in nature")]
                for path in paths]


class PhaseCSemanticPipelineTest(unittest.TestCase):
    def test_batch_quality_tags_model_and_evaluation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            temple = root / "temple-sunset.jpg"
            bird = root / "bird-forest.jpg"
            Image.new("RGB", (32, 24), (230, 150, 70)).save(temple)
            Image.new("RGB", (32, 24), (30, 110, 60)).save(bird)
            store = AssetStore(root / "ai.sqlite3")
            store.enqueue_asset({"asset_id": "temple-1", "object_key": "/travel/temple.jpg",
                                 "local_path": str(temple)})
            store.enqueue_asset({"asset_id": "bird-1", "object_key": "/nature/bird.jpg",
                                 "local_path": str(bird)})
            worker = AssetWorker(store, SemanticStubProvider(), batch_size=2)
            self.assertEqual(worker.process_batch(), 2)

            asset = store.get_asset("temple-1")
            self.assertEqual(asset["embedding_model"], "semantic-stub@v1")
            self.assertEqual(asset["metadata"]["semantic_tags"], ["寺庙"])
            self.assertEqual(asset["metadata"]["quality_method"], "pillow-heuristic-v1")
            self.assertEqual(asset["metadata"]["media_kind"], "raster_image")

            report = evaluate_retrieval(
                store, SemanticStubProvider(),
                [RetrievalCase("寺庙日落", ("temple-1",)), RetrievalCase("bird", ("bird-1",))],
                top_k=1,
            )
            self.assertEqual(report["recall_at_k"], 1.0)
            self.assertEqual(report["mrr_at_k"], 1.0)

    def test_quality_hash_and_non_raster_semantic_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            image = root / "temple.jpg"
            Image.new("RGB", (16, 16), (100, 100, 100)).save(image)
            metadata = analyze_media(image)
            self.assertEqual(hash_distance(metadata["perceptual_hash"], metadata["perceptual_hash"]), 0)

            video = root / "clip.mp4"
            video.write_bytes(b"not-a-real-video")
            store = AssetStore(root / "ai.sqlite3")
            job_id = store.enqueue_asset({"asset_id": "video-1", "object_key": "/clip.mp4",
                                          "local_path": str(video)})
            self.assertEqual(AssetWorker(store, SemanticStubProvider()).process_batch(), 1)
            self.assertEqual(store.job(job_id)["status"], "failed")
            self.assertIn("RAW/video require", store.job(job_id)["last_error"])


if __name__ == "__main__":
    unittest.main()
