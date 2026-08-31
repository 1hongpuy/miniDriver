# CineLake Phase D Lite：可分页查询服务、观测与基准报告（2026-08-21）

## 结论

Phase D Lite 已将 AI-App-Lite 的“单次 SQLite 全量搜索演示”整理为有明确 API 契约的查询服务：支持
跨目录结果、结构化过滤、稳定 keyset 分页、查询与索引观测，并且不再把 embedding 或 Worker 本地文件路径
返回给浏览器。

它**不是** PostgreSQL/pgvector/Milvus 的替代品。当前 SQLite 仍是单进程持久化真源，向量召回仍是精确
线性扫描；新增的内存快照只降低重复 JSON 解码成本。真实 56 张摄影集上，四并发查询的执行 QPS 从
**13.53** 提升到 **51.70**，但 P95 仍为 **164.514 ms**，说明下一次规模升级必须迁移到数据库侧向量
检索或原生 ANN，不能仅继续扩展 Python 缓存。

## 1. 目标与范围

Phase C 已证明图片能够被 OpenCLIP 索引和检索；Phase D 要解决的是查询结果如何以可演进、可观测的方式
提供给前端：

```text
文字查询 + filters
  → 同模型 text embedding
  → 可见/结构化条件过滤
  → 向量 Top-K
  → score DESC, asset_id ASC 稳定排序
  → keyset cursor + 脱敏结果 + 指标
```

本阶段完成：

- `POST /ai/search` 支持 `page_size`（1--50）、`cursor` 和 `filters`；旧 `top_k` 仍兼容；
- `path_prefix`、`semantic_tags`、`quality_min`、`capture_time_from/to` 及既有 EXIF metadata filter；
- 以 `score DESC, asset_id ASC` 排序，Cursor 绑定 query vector、filter、embedding model 与 index snapshot；
- `GET /ai/metrics` 返回 bounded query P50/P95、错误数、结果数与 index freshness；
- `www-v2` 接入“加载更多”，保留跨目录对象路径；
- Ready Asset 的只读不可变快照缓存；Worker 成功写入后在同一进程使其失效；
- `benchmark-query` 命令，用于可重复记录 query-executor 并发基准。

明确不做：鉴权/Dataset 可见范围、缩略图派生、跨进程缓存失效、PostgreSQL、pgvector、HNSW、Milvus 与
cross-encoder rerank。结果中的 `thumbnail.state=unavailable` 是显式边界，不伪装为已生成缩略图。

## 2. 架构与数据边界

```text
www-v2
  └─ POST /ai/search { query, filters, page_size, cursor }
       │
       ▼
AI HTTP Handler
  ├─ OpenCLIP / Hash Provider: text embedding
  ├─ AssetStore.search_page()
  │    ├─ SQLite: assets/jobs 的持久化真源
  │    ├─ immutable ready-asset snapshot（本进程只读缓存）
  │    ├─ metadata pre-filter
  │    └─ exact cosine scan + keyset cursor
  └─ 脱敏 JSON: object route / metadata / score / thumbnail state
       │
       ▼
前端结果卡片 + “加载更多”
```

索引写路径保持不变：`Gateway Commit → Redis Stream → SQLite Job → Worker → upsert_asset`。Worker 只在
成功 `upsert_asset` 后清空本进程查询快照，因此查询不会读取半完成 Asset；多进程部署时没有 cache
invalidation bus，必须改用 PostgreSQL/pgvector 或显式版本广播。

## 3. 查询 API 契约

请求：

```json
{
  "query": "鸟类",
  "page_size": 9,
  "filters": {
    "path_prefix": "/自然",
    "semantic_tags": ["鸟类"],
    "quality_min": 0.6
  },
  "cursor": "optional-keyset-cursor"
}
```

返回中的关键字段：

```json
{
  "embedding_model": "openclip:ViT-B-32-quickgelu:openai",
  "results": [{
    "asset_id": "...",
    "object_key": "/自然/bird.jpg",
    "object_version": 1,
    "score": 0.82,
    "metadata": {"semantic_tags": ["鸟类"]},
    "object_route": {"object_id": "...", "object_key": "/自然/bird.jpg"},
    "thumbnail": {"state": "unavailable", "url": null}
  }],
  "page": {
    "next_cursor": "...",
    "total_candidates": 8,
    "stable_sort": "score_desc,asset_id_asc",
    "snapshot_updated_at": 1787270000.0
  },
  "query_latency_ms": 7.8
}
```

Cursor 中保存末尾 `(score, asset_id)`、snapshot 上限和 query fingerprint。fingerprint 由向量、filter 与
model 计算；将一个查询的 cursor 用于另一查询/模型/过滤条件会返回 `400`，而不是生成偏移错误的下一页。
在 snapshot 未改变时，keyset 规则不会重复或跳过对象。后续权限范围加入时，调用者 identity 也必须进入
fingerprint。

`GET /ai/metrics` 返回最近最多 512 次请求的 query P50/P95/max、错误数、结果数，以及 `ready_assets`、
`newest_indexed_at`、`index_freshness_seconds` 和 Job 状态计数。它是开发观测接口，不替代 Prometheus。

## 4. 真实摄影集基准

### 条件

- Phase C 的本地摄影集：56 张 JPEG、7 个中文类别，每类 8 张；
- 已索引模型：`openclip:ViT-B-32-quickgelu:openai`，512 维；
- 8 vCPU、约 3.5 GiB RAM、无 GPU；
- 查询：`鸟类`，Top-5，100 requests，4 concurrency；
- `embedding_once_ms` 只生成一次文字向量；`query_execution_ms` 不含 HTTP 和重复文字 embedding。

### 结果

| 实现 | 并发 / 请求 | Query P50 / P95 / Max | QPS | 结论 |
|---|---:|---:|---:|---|
| 旧路径：每请求 SQLite + JSON 解码 | 4 / 100 | 256.604 / 672.008 / 870.281 ms | 13.534 | 重复反序列化主导，不能用于并发服务 |
| Phase D Lite：ready snapshot 缓存 | 4 / 100 | 70.302 / 164.514 / 189.053 ms | 51.696 | QPS 约 3.8 倍，但 Python CPU 尾部仍明显 |
| Phase D Lite：ready snapshot 缓存 | 1 / 50 | 7.752 / 17.366 / 27.999 ms | 105.695 | 单查询足够快，4 并发因 GIL/逐元素余弦竞争而回落 |

两次模型已加载后的 `embed_text("鸟类")` 分别约 91.033 ms 和 87.703 ms；首次冷模型运行曾约 629 ms，
因此模型加载与 query executor 必须分开记录。结果仅覆盖 56 个向量、单机内存快照，不外推为 pgvector 或
大规模 ANN 性能。

基准命令：

```bash
PYTHONPATH=ai-app-lite \
AI_APP_DB=/tmp/minidriver-phase-c-20260821/ai.sqlite3 \
AI_EMBEDDING_PROVIDER=openclip \
AI_OPENCLIP_MODEL=ViT-B-32-quickgelu \
AI_OPENCLIP_PRETRAINED=openai \
ai-app-lite/.venv/bin/python -m cinedata_ai.main benchmark-query \
  --query 鸟类 --requests 100 --concurrency 4 --page-size 5
```

## 5. 测试

新增 `test_phase_d_query_service.py`：

- 同分数按 `asset_id` 稳定排序，两个页面无重复；
- Cursor 不能混用于另一 query；
- `path_prefix`、semantic tag 和 quality filter 可组合；
- Handler 返回不含 `embedding` 或 `local_path`，并显式返回 thumbnail state；
- QueryMetrics 和 index stats 可读取。

当前 Python 测试：**9 通过，1 跳过**；跳过项为需要独立 Redis 的既有集成测试。受执行环境 socket 限制，
HTTP Handler 使用无 socket 契约单测覆盖；实际 API 可用性需在本机按 README 启动后用 curl 验证。

## 6. 迁移判据与下一步

Phase D Lite 已把外部契约拆清楚，后续迁移不应改前端分页语义：

```text
SQLite snapshot scan
  → PostgreSQL assets/metadata transaction
  → pgvector exact scan 或 HNSW
  → 数据量、filter 选择性、Recall@K、P95 明确不足后再评估 Milvus/Faiss
```

触发 PostgreSQL/pgvector 的合理条件是：真实对象数量和并发继续增长，或要求多 AI API 进程共享一致索引时。
迁移评测必须固定模型、素材集、filter、Top-K 和 concurrency，同时记录 Recall@K、P50/P95、索引时间和
数据库大小；不能只用一次更快的查询宣称系统更好。

下一功能阶段应优先补派生缩略图/RAW preview 与对象级授权，再进入 Dataset、版本和 lineage；它们会让
Phase D 的 `object_route`、version 和 metadata filter 变成真正可治理的数据资产接口。
