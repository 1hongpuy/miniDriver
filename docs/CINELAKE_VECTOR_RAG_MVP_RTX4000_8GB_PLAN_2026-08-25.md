# CineLake（miniAI）向量检索与本地 RAG MVP：RTX 4000 8 GiB 计划（2026-08-25）

> 目标：先跑通“对象上传 → 异步索引 → 向量搜索 → 本地 RAG 回答”的最小闭环。  
> 显存假设：按**实际可用显存约 8 GiB**保守设计；部署前必须用 `nvidia-smi` 核实具体 RTX 4000 型号、驱动和剩余显存。  
> 关键约束：8 GiB 不应默认常驻两个完整 GPU 模型；先采用单 GPU 调度与顺序运行，测量后才允许共存。

## 1. MVP 范围

```text
输入：MiniDriver 已 Commit 的 JPEG；RAW 先只处理可提取 preview 的样本。
存储：MiniDriver 原图；PostgreSQL + pgvector 保存 Asset、状态、向量和审计。
检索：中文文本 → embedding → pgvector Top-K → 前端展示原对象引用。
RAG：Top-K 的已授权证据 → 本地文本 LLM → 带 objectId/version 引用的回答。
不做：多 GPU、视频、OCR、自动删除、S3Connector、Spark、TensorRT。
```

## 2. 最小端到端链路

```text
1. Browser 上传 JPEG 到 MiniDriver。
2. File COMMITTED 后，Gateway Outbox 发布 FILE_UPLOAD_COMMITTED。
3. CineLake stream-worker 在 PostgreSQL 创建幂等 IndexJob，成功后 ACK Redis。
4. CPU worker 读取对象，生成 EXIF/缩略图/质量数据。
5. GPU embedding worker 以小 batch 生成 image embedding，写入 pgvector。
6. Browser 调 POST /ai/search，服务生成 text embedding，pgvector 返回 Top-K Asset。
7. Browser 用 objectId 回 MiniDriver Gateway 预览原图。
8. Browser 调 POST /ai/rag/query；RAG 只读取 Top-K evidence，调用本地 LLM 回答。
```

这里有两种完全不同的 GPU 工作：

```text
Embedding：大量离线图片，追求 batch 吞吐；不需要大语言模型。
RAG generation：少量在线问答，追求首 token 延迟和可流式输出。
```

它们不能和上传数据面耦合，也不应默认抢同一块 8 GiB 显存。

## 3. 向量检索实现

### 3.1 Image embedding

```text
MiniDriver object bytes / preview
  → CPU decode + resize
  → image embedding model
  → L2 normalize vector
  → INSERT/UPSERT embeddings(asset_id, object_version, model_id, vector)
```

现有 `ai-app-lite` 已有 hash smoke provider 和可选 OpenCLIP provider。MVP 应把真实视觉 embedding 模型配置成
唯一 `model_id`，并用已有摄影测试集测 Recall@K；不要把不同模型的向量混在同一次相似度比较中。

对于中文照片搜索，视觉/文本 embedding 模型必须通过中文查询集实测；“模型能加载”不等于中文语义效果可接受。

### 3.2 pgvector

初始使用 PostgreSQL + pgvector：

```text
embeddings
  asset_id / object_version / model_id / vector / created_at

query
  filter READY assets + compatible model_id
  → vector Top-N
  → score DESC, asset_id ASC
  → 返回 objectId/objectVersion/metadata/score
```

数据量与延迟确实证明需要时，再为常用模型建立 HNSW/IVFFlat 索引并测 Recall@K/P95；不要在 MVP 前迁移 Milvus。

### 3.3 文本查询

```text
"暖色寺庙照片"
  → same-model text encoder
  → one query vector
  → pgvector ANN Top-K
```

查询期间不读取所有原图，也不向浏览器、Gateway、LLM 传输全库向量。

## 4. RAG 的最小实现

RAG 建立在检索之后：

```text
POST /ai/rag/query
  { caller, query, filters, page_size }

1. 复用 /ai/search，取 Top 10～20。
2. 用质量/时间/权限筛成 Top 3～5 evidence。
3. 构造受限 prompt：用户问题 + evidence JSON。
4. 调用本地 LLM 的 OpenAI-compatible API。
5. 返回 answer + evidence[]，并写 agent_run audit。
```

示例 evidence：

```json
{
  "object_id": "obj-123",
  "object_version": 3,
  "object_key": "/travel/temple-sunset.jpg",
  "tags": ["寺庙", "日落"],
  "quality_score": 0.84,
  "retrieval_score": 0.91
}
```

第一版 LLM 不接收原图，只回答已索引的 metadata/标签/质量/检索结果，且明确要求“证据不足时回答不知道”。
之后若接入 VLM，再只允许它看 Top-K 缩略图，而不是整个素材库。

## 5. RTX 4000 8 GiB 的模型与调度决策

### 5.1 不能默认两个模型同时常驻

8 GiB 显存不仅要放模型权重，还需要 CUDA context、激活、图像预处理缓冲、KV cache、运行时 workspace 和碎片余量。
因此以下做法不作为 MVP 默认：

```text
一个视觉 embedding 大模型常驻 GPU
  + 一个 4B/7B LLM 常驻 GPU
  + 还允许多个 RAG 请求和批量图片任务并发
```

这很容易 OOM 或由于 KV cache 抖动导致不稳定。vLLM 的示例也将 `gpu_memory_utilization` 作为显式资源上限；它可支持
pooling/embedding 模型，但这不代表 8 GiB 可以无成本共存多个模型。
[vLLM embedding/pooling 示例](https://docs.vllm.ai/en/v0.13.0/examples/pooling/token_embed/)。

### 5.2 MVP 推荐：单 GPU Broker，时间隔离

```text
GpuBroker（一个进程/一个互斥 lease）
  ├─ embedding mode：GPU ImageEmbeddingWorker 按 batch 消化任务
  └─ rag mode：GPU LLM Server 接受少量在线 RAG 请求
```

默认策略：

```text
优先保证在线 RAG：RAG 到达时停止领取新的 embedding batch。
当前 batch 结束后释放 GPU lease，RAG 获得 lease。
RAG 空闲一段时间后，embedding 继续。
```

模型切换会有加载延迟，但对 MVP 更稳定、更容易解释。若希望 RAG 永远即时，另一种保守选择是：

```text
GPU：仅运行 embedding worker。
CPU：运行小型量化文本 LLM 做低 QPS RAG。
```

性能测量证明显存足够后，才可尝试常驻一个小 embedding 模型和量化 LLM；必须给 LLM 保留 KV cache 余量，并设置
最大并发请求、最大上下文和最大输出 token。

### 5.3 模型候选（先做评测，不作性能承诺）

| 用途 | MVP 候选 | 8 GiB 规则 |
|---|---|---|
| 图片/文本检索 | 当前 OpenCLIP provider；选择中文/多语言效果经测试合格的视觉 embedding 模型 | 优先作为独立 batch Worker；模型小、输入缩放固定。 |
| 文本 RAG 回答 | `Qwen/Qwen3-4B` 的兼容量化部署 | 只用于 text RAG；使用 4-bit/低显存配置，并严格限制 KV cache、上下文和并发。 |
| 图片描述/OCR（后续） | `Qwen/Qwen2.5-VL-3B-Instruct` 的兼容量化部署 | 仅离线 Top-K/批任务；不与 RAG LLM 默认共存。 |

`Qwen3-4B` 是文本生成模型，可用于本地 RAG 回答；其官方模型卡提供 Transformers 加载方式。
[Qwen3-4B 模型卡](https://huggingface.co/Qwen/Qwen3-4B)。

`Qwen2.5-VL-3B-Instruct` 可理解图像和文本，但其官方卡显示的是 BF16 权重，且图像分辨率越高，计算/显存代价越大；
在 8 GiB 环境中只能作为**量化、低分辨率、单任务**的后续实验，不能作为第一版检索的必经依赖。
[Qwen2.5-VL-3B-Instruct 模型卡](https://huggingface.co/Qwen/Qwen2.5-VL-3B-Instruct)。

注意：量化格式、vLLM/llama.cpp/Transformers 对该模型的兼容性和实际显存占用必须在目标机器测试后记录；文档不承诺
某个模型一定能以某个并发度装入 8 GiB。

## 6. 进程边界与 API

```text
CineLake API            :18290
  POST /ai/search
  POST /ai/rag/query

Embedding Worker
  Redis/PostgreSQL Job → GPU/CPU model → PostgreSQL embedding

LLM Server（可选）
  OpenAI-compatible /v1/chat/completions
  仅接受 CineLake API 的内网请求

GpuBroker
  分配 embedding / rag GPU lease；不处理对象存储逻辑
```

RAG API 不直接调用 MiniDriver DataNode；它只使用 CineLake 已持久化的 Top-K evidence。若未来 VLM 需要缩略图，
由 CineLake 的受控 StorageConnector 取 `derivative`，并限制数量、大小和超时。

## 7. 开发顺序

```text
M0：当前 hash/OpenCLIP 检索闭环 + PostgreSQL/pgvector。

M1：真实本地图文 embedding、固定中文图片测试集、Recall@K/MRR。

M2：缩略图/EXIF 与 embedding Job 分离；Backfill checkpoint、优先级与有界 GPU batch。

M3：/ai/rag/query，先用 mock LLM 验证 evidence/prompt/audit；
    再接本地 Qwen3-4B 量化服务。

M4：GpuBroker、并发上限、P50/P95、GPU 显存/利用率、失败重试。

M5：可选 VLM caption/OCR、模型升级重索引、S3Connector。
```

## 8. MVP 验收

```text
上传 JPEG 后，用户无需手工登记 Asset。
GPU 忙时，MiniDriver 上传/下载仍可完成；Asset 显示 GPU_QUEUED。
中文查询返回经过人工验证的相关图片，且结果能打开固定 objectVersion。
重复事件/重试不生成重复 embedding。
RAG 回答包含 evidence，不产生无来源的图片引用。
8 GiB GPU 下 embedding 与 RAG 不发生 OOM；超载返回队列/限流状态，而非无界堆积。
```

## 9. 当前不做的优化

```text
TensorRT / CUDA kernel：等模型、输入尺寸、batch 和指标稳定后再做。
Spark：等历史回填规模真正需要分布式批计算后再做。
Kafka：等 Redis Streams 的吞吐、保留或多消费者成为证据明确的瓶颈后再做。
多 GPU：等单 GPU 的显存、吞吐、队列和模型质量均有基线后再做。
```

MVP 的成功不是模型最多，而是证明一个完整、可恢复、可观测的系统闭环。
