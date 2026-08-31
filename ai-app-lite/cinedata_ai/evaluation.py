from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .embeddings import EmbeddingProvider
from .store import AssetStore


@dataclass(frozen=True)
class RetrievalCase:
    query: str
    expected_asset_ids: tuple[str, ...]
    filters: dict[str, Any] | None = None


def load_cases(path: Path) -> list[RetrievalCase]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("evaluation cases must be a JSON array")
    cases: list[RetrievalCase] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("each evaluation case must be an object")
        query = str(item.get("query", "")).strip()
        expected = tuple(str(value) for value in item.get("expected_asset_ids", []) if str(value))
        if not query or not expected:
            raise ValueError("each case requires query and expected_asset_ids")
        filters = item.get("filters")
        if filters is not None and not isinstance(filters, dict):
            raise ValueError("filters must be an object")
        cases.append(RetrievalCase(query, expected, filters))
    return cases


def evaluate_retrieval(store: AssetStore, provider: EmbeddingProvider,
                       cases: Iterable[RetrievalCase], top_k: int = 5) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for case in cases:
        results = store.search(provider.embed_text(case.query), top_k, case.filters, provider.model_id)
        actual = [str(result["asset_id"]) for result in results]
        expected = set(case.expected_asset_ids)
        hit_ranks = [index + 1 for index, asset_id in enumerate(actual) if asset_id in expected]
        relevant_count = sum(1 for asset_id in actual if asset_id in expected)
        rows.append({
            "query": case.query,
            "expected_asset_ids": list(case.expected_asset_ids),
            "actual_asset_ids": actual,
            "hit": bool(hit_ranks),
            "first_hit_rank": min(hit_ranks) if hit_ranks else None,
            "recall_at_k": relevant_count / len(expected),
        })
    total = len(rows)
    hits = sum(1 for row in rows if row["hit"])
    reciprocal_rank = sum(1.0 / row["first_hit_rank"] for row in rows if row["first_hit_rank"])
    return {
        "embedding_model": provider.model_id,
        "top_k": top_k,
        "case_count": total,
        "recall_at_k": sum(row["recall_at_k"] for row in rows) / total if total else 0.0,
        "query_hit_rate_at_k": hits / total if total else 0.0,
        "mrr_at_k": reciprocal_rank / total if total else 0.0,
        "cases": rows,
    }
