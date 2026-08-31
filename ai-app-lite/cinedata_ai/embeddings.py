from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


@dataclass(frozen=True)
class TagPrediction:
    label: str
    score: float
    prompt: str


# Chinese display labels remain stable for the web UI; English prompts are used
# for the default OpenAI-pretrained CLIP checkpoint.
DEFAULT_ZERO_SHOT_LABELS: tuple[tuple[str, str], ...] = (
    ("日落", "a warm sunset landscape"),
    ("山景", "a mountain landscape"),
    ("寺庙", "a temple or historic shrine"),
    ("城市夜景", "a city skyline at night"),
    ("人像", "a portrait photograph of a person"),
    ("鸟类", "a bird in nature"),
    ("海岸", "a coast or beach landscape"),
    ("森林", "a forest landscape"),
    ("天空", "a dramatic sky with clouds"),
)

_OPENCLIP_QUERY_ALIASES = {
    "日落": "a warm sunset landscape",
    "山景": "a mountain landscape",
    "佛像": "a buddhist statue",
    "寺庙": "a temple or historic shrine",
    "佛像或寺庙": "a buddhist statue or temple",
    "城市夜景": "a city skyline at night",
    "人像": "a portrait photograph of a person",
    "鸟类": "a bird in nature",
    "海岸": "a coast or beach landscape",
    "森林": "a forest landscape",
    "天空": "a dramatic sky with clouds",
}


class EmbeddingProvider:
    name = "base"
    model_id = "base@unknown"
    supports_semantic_tags = False

    def embed_image(self, path: Path, text_hint: str = "") -> list[float]:
        raise NotImplementedError

    def embed_text(self, text: str) -> list[float]:
        raise NotImplementedError

    def embed_images(self, paths: Sequence[Path], text_hints: Sequence[str]) -> list[list[float]]:
        if len(paths) != len(text_hints):
            raise ValueError("paths and text_hints must have the same length")
        return [self.embed_image(path, hint) for path, hint in zip(paths, text_hints)]

    def classify_images(self, paths: Sequence[Path], top_k: int = 3,
                        min_score: float = 0.20) -> list[list[TagPrediction]]:
        del top_k, min_score
        return [[] for _ in paths]


class HashEmbeddingProvider(EmbeddingProvider):
    """Deterministic, dependency-free smoke provider.

    It is intentionally not presented as a semantic model. It makes the complete
    ingestion/search/caption pipeline runnable before OpenCLIP is installed.
    """

    name = "hash-smoke"

    def __init__(self, dimension: int = 256):
        self.dimension = dimension
        self.model_id = f"hash-smoke@sha256-token-v1/d{dimension}"

    def embed_image(self, path: Path, text_hint: str = "") -> list[float]:
        digest = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else str(path)
        return self._vector(f"image:{digest} {text_hint}")

    def embed_text(self, text: str) -> list[float]:
        return self._vector(f"text:{text}")

    def _vector(self, text: str) -> list[float]:
        vector = [0.0] * self.dimension
        tokens = re.findall(r"[\w\u4e00-\u9fff]+", text.lower())
        if not tokens:
            tokens = [text]
        for token in tokens:
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            for offset in range(0, len(digest), 2):
                index = int.from_bytes(digest[offset : offset + 2], "big") % self.dimension
                vector[index] += 1.0 if digest[offset] & 1 else -1.0
        norm = math.sqrt(sum(value * value for value in vector)) or 1.0
        return [value / norm for value in vector]


class OpenClipEmbeddingProvider(EmbeddingProvider):
    """Optional real image/text provider; loaded only when OpenCLIP is installed."""

    name = "openclip"
    supports_semantic_tags = True

    def __init__(self, model_name: str = "ViT-B-32", pretrained: str = "openai"):
        try:
            import open_clip  # type: ignore
            import torch  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "OpenCLIP provider requires 'torch' and 'open_clip_torch'; "
                "use AI_EMBEDDING_PROVIDER=hash for smoke mode"
            ) from exc
        self._torch = torch
        self._open_clip = open_clip
        self._model, _, self._preprocess = open_clip.create_model_and_transforms(
            model_name, pretrained=pretrained
        )
        self._tokenizer = open_clip.get_tokenizer(model_name)
        self._model.eval()
        self.model_id = f"openclip:{model_name}:{pretrained}"

    def embed_image(self, path: Path, text_hint: str = "") -> list[float]:
        del text_hint
        return self.embed_images([path], [""])[0]

    def embed_images(self, paths: Sequence[Path], text_hints: Sequence[str]) -> list[list[float]]:
        del text_hints
        if not paths:
            return []
        from PIL import Image

        images = []
        for path in paths:
            with Image.open(path) as source:
                images.append(self._preprocess(source.convert("RGB")))
        batch = self._torch.stack(images)
        with self._torch.no_grad():
            vectors = self._model.encode_image(batch)
        return [self._normalise(vector.tolist()) for vector in vectors]

    def embed_text(self, text: str) -> list[float]:
        # The default OpenAI CLIP checkpoint is trained predominantly on English.
        # Keep a small explicit UI-label mapping rather than claiming universal
        # Chinese text understanding; arbitrary Chinese queries remain unchanged.
        prompt = _OPENCLIP_QUERY_ALIASES.get(text.strip(), text)
        tokens = self._tokenizer([prompt])
        with self._torch.no_grad():
            vector = self._model.encode_text(tokens)
        return self._normalise(vector[0].tolist())

    def classify_images(self, paths: Sequence[Path], top_k: int = 3,
                        min_score: float = 0.20) -> list[list[TagPrediction]]:
        if not paths:
            return []
        prompts = [prompt for _label, prompt in DEFAULT_ZERO_SHOT_LABELS]
        tokens = self._tokenizer(prompts)
        with self._torch.no_grad():
            text_features = self._model.encode_text(tokens)
        text_vectors = [self._normalise(vector.tolist()) for vector in text_features]
        image_vectors = self.embed_images(paths, [""] * len(paths))
        output: list[list[TagPrediction]] = []
        for image_vector in image_vectors:
            scored = [
                TagPrediction(label, sum(a * b for a, b in zip(image_vector, text_vector)), prompt)
                for (label, prompt), text_vector in zip(DEFAULT_ZERO_SHOT_LABELS, text_vectors)
            ]
            scored.sort(key=lambda item: item.score, reverse=True)
            output.append([item for item in scored[:max(1, top_k)] if item.score >= min_score])
        return output

    @staticmethod
    def _normalise(values: Iterable[float]) -> list[float]:
        result = [float(value) for value in values]
        norm = math.sqrt(sum(value * value for value in result)) or 1.0
        return [value / norm for value in result]


def create_provider(name: str, dimension: int = 256, model_name: str = "ViT-B-32-quickgelu",
                    pretrained: str = "openai") -> EmbeddingProvider:
    if name.lower() in {"openclip", "clip"}:
        return OpenClipEmbeddingProvider(model_name, pretrained)
    return HashEmbeddingProvider(dimension)
