# miniAI / LensAI：个人多模态数据搜索与 Agent 平台完整设计（2026-08-25）

> 产品名建议：**LensAI**。代码项目名：**miniAI**；现有学习型实现目录：`ai-app-lite/`，历史文档代号为
> CineLake。三者在本设计中指向同一 AI 数据平台，不是三个独立服务。
>
> 定位：面向个人照片、RAW、视频和文档的多模态数据资产平台。它从对象存储中读取已提交版本，异步产生派生数据，
> 提供检索、解释与受控 Agent；它不是另一个对象存储，也不是“只为了聊天而做的 RAG”。

## 1. 一句话目标

让用户不必记住文件夹和文件名，而能以自然语言、结构化条件或图片相似性找到自己的数据，并得到可追溯的回答：

```text
“找去年云南旅行拍的日落照片，挑三张适合做封面，并说明选择理由。”
```

输出必须始终可以回到一个明确证据：

```text
storage source + objectId + objectVersion + metadata/model result
```

不是让模型“猜一张照片存在”，而是让模型在用户有权访问的对象证据上做搜索、排序、说明和有限的工作流编排。

## 2. 项目边界与双项目关系

```text
                 用户 / Web / future mobile client
                       │              │
           upload/download              search / RAG / Agent
                       │              │
                       ▼              ▼
                 MiniDriver       miniAI / LensAI
              raw object truth    derived-data truth
                       │              │
        JPEG / RAW / video bytes   metadata/vector/audit/dataset
```

| 问题 | MiniDriver | miniAI / LensAI |
|---|---|---|
| 原图在哪里、是否有副本 | 负责 | 不负责 |
| 上传是否完成、能否读取 | 负责 | 只消费已 Commit 结果 |
| 缩略图、EXIF、质量、caption、tag | 不负责 | 负责派生和版本 |
| 向量、相似搜索、结构化过滤 | 不负责 | 负责 |
| LLM/VLM 回答与 Agent 审计 | 不负责 | 负责 |
| GPU 模型、批处理和队列 | 不负责 | 负责 |

miniAI 必须通过 `StorageConnector` 接入存储，因而可支持 MiniDriver 专用 HTTP，也可支持未来 MinIO/S3：

```text
MinidriverHttpConnector   manifest / controlled read / committed event
S3Connector               HEAD + GET(versionId) / S3 event / SDK credentials
```

对象协议不同不影响 miniAI 的业务主键：

```text
AssetIdentity = source + objectId + objectVersion
EmbeddingIdentity = AssetIdentity + modality + modelId
DerivativeIdentity = AssetIdentity + derivativeType + processorVersion
```

## 3. 目标总架构

```text
                                User / Web
                         upload │     │ query / agent
                                ▼     ▼
                       Object Storage   Query API
                    MiniDriver / S3     ├─ auth/scope (future)
                                │       ├─ metadata filter
                   committed event       ├─ vector retrieval
                                ▼       └─ RAG evidence assembly
                         Event Ingestor             │
                                │                   ▼
                           durable IndexJob    Local/remote LLM
                                │
            ┌───────────────────┼─────────────────────┐
            ▼                   ▼                     ▼
       CPU pipeline         GPU embedding         VLM/OCR later
  EXIF/preview/thumb/quality  image/text vectors    caption/tags
            └───────────────────┼─────────────────────┘
                                ▼
                  PostgreSQL + pgvector + derivatives store
                 assets / jobs / vectors / datasets / audit
```

### 3.1 三种完全不同的流

不要把它们当成同一件事：

```text
写入流：Browser → MiniDriver → Object COMMITTED
索引流：Commit event → durable job → CPU/GPU derived data
查询流：Query → one query vector → DB Top-K → optional LLM answer
```

上传成功从不等待 GPU。索引流可重试且有界。查询流不读取全库图片或向量：只把一个 query vector 发送给数据库，
数据库在服务端返回少量 Top-K 结果。

## 4. 关键数据模型

```text
assets
  asset_id / source / object_id / object_version / object_key
  content_type / content_hash / index_state / metadata_json / updated_at

derivatives
  asset_id / object_version / derivative_type / processor_version
  storage_source / location / width / height / content_type
  checksum / byte_size / state / created_at

embeddings
  asset_id / object_version / modality / model_id / vector / created_at

index_jobs
  job_id / idempotency_key / priority / state / attempts / retry_at
  source_event_id / last_error

datasets / dataset_members
  dataset_id / asset_id / object_version / role

agent_runs / agent_run_results
  caller / query / filter / model / evidence(asset/version/rank/score)
```

`IndexJob` 是最终任务真相；Redis/Kafka 只是投递媒介。一个典型幂等键：

```text
minidriver/{objectId}/v{objectVersion}/image-embedding/{modelId}
```

重复事件、Worker 崩溃或模型重试都只能重用/继续这个任务，而不是制造无限副本向量。

### 4.1 缩略图、向量与缓存不是同一种存储

三者解决不同问题，不能互相替代：

| 数据 | 持久化位置 | 查询时的作用 | 能否只放 Redis |
|---|---|---|---|
| 原图 / RAW | MiniDriver 或 S3 对象存储 | 用户明确打开、下载或后续重处理 | 不可以 |
| 缩略图 / preview | `derivatives/` 对象存储命名空间 | 搜索结果卡片、低成本预览 | 不可以 |
| 缩略图的 location、版本、checksum | PostgreSQL `derivatives` 表 | 定位、校验、状态与授权 | 不适用 |
| 图像 embedding | PostgreSQL + pgvector | 服务端向量 Top-K | 不可以 |
| 热门缩略图字节 / 短期 URL | Redis、浏览器缓存或未来 CDN | 加速重复展示 | 可以，但只能作为缓存 |
| 热门搜索 Top-K 列表 | Redis | 跳过重复向量查询/排序 | 可以，但必须短期缓存 |

目标派生路径如下：

```text
objectId/v3 原图
  → CPU Worker 生成 thumbnail.webp
  → 持久化到 derivatives/objectId/v3/thumbnail.webp
  → PostgreSQL 写入 derivative metadata（location/checksum/processorVersion/READY）
  → 可选写入 Redis thumbnail cache
```

因此 Redis 重启、淘汰或缓存失效时，缩略图并不会丢失：服务只需回源读取持久化的 derivative 对象，并可再次填充缓存。

## 5. 核心处理流程

### 5.1 新上传：先存好，再慢慢理解

```text
1. Browser 上传原图到 MiniDriver。
2. 仅在 File COMMITTED 后，Gateway Outbox 可靠发布完成事件。
3. Ingestor 先在 PostgreSQL 持久化 event ledger + IndexJob，再 ACK Redis。
4. CPU Worker 拉取固定对象版本，提取 EXIF、preview、缩略图、质量和 pHash。
5. GPU Embedding Worker 用小 batch 读取 preview/图片，生成归一化向量。
6. 完整写入 Asset、Derivative、Embedding 后，Asset = READY。
```

前端应分别展示：

```text
Object: COMMITTED（原图已经可靠可用）
AI:     PENDING / CPU_PROCESSING / GPU_QUEUED / READY / FAILED
```

### 5.2 查询时不重新处理原图：向量检索与缩略图回显

首次索引会读取对象一次，用于生成派生物；之后的普通搜索不应再次拉取原图或重算图像向量：

```text
用户输入“云南日落”
  → 文本模型仅生成一个 query vector
  → pgvector 在服务端找 image embedding Top-K
  → PostgreSQL 补齐 Asset、Derivative location、标签与对象版本
  → 前端加载这 K 张已生成的 thumbnail.webp
  → 用户点击某张结果时，才读取 MiniDriver 原图
```

这里的 `image embedding` 在 `(source, objectId, objectVersion, modelId)` 不变时只生成一次；模型升级或对象内容变为
新版本才创建新的 `IndexJob`。缩略图同理：`processorVersion` 相同则复用，缩略图处理器升级才重新生成。

### 5.3 缓存策略：缓存单张缩略图优先于缓存一次搜索

缓存不承担真相，只减少重复网络和数据库工作：

```text
浏览器 HTTP cache
  → Redis thumbnail:{assetId}:{objectVersion}:{processorVersion}
  → derivatives object storage（持久化回源）
```

单张缩略图缓存复用率通常最高：不同用户、不同查询只要命中同一 Asset 都可复用。可选的搜索结果缓存是：

```text
search:{scopeHash}:{queryHash}:{filterHash}:{modelId}:{indexVersion}
  → [{assetId, objectVersion, score, thumbnailVersion}, ...]
```

它只保存 Top-K 的小型引用和分数，不复制图片 Body；必须有短 TTL，并将用户/租户权限范围、filter、模型和索引版本
纳入 key。否则会出现越权结果、模型切换后混用结果，或索引更新后长期陈旧的问题。

### 5.4 历史回填：有 checkpoint 的后台作业

大量既有图片不能“启动时全部下载到 GPU”：

```text
Catalog Scanner（分页 + checkpoint）
  → 对每个已 COMMITTED 版本检查 IndexJob 幂等键
  → READY 跳过；缺失则入队
  → CPU/GPU Worker 在自身并发、字节和显存限制内领取任务
```

新上传使用 `interactive` 优先级；历史回填、模型升级重算使用 `background`，不能反过来饿死用户刚上传的照片。

## 6. 检索、RAG 与 Agent 的层次

### 6.1 多模态搜索：不需要大语言模型

```text
“云南 日落”
  → text embedding（与图片 embedding 同一 modelId）
  → SQL filter：owner/time/camera/folder/Dataset/status
  → pgvector Top-100
  → score + quality + time rerank
  → Top-K object references
```

MVP 先使用 similarity score；VLM rerank 仅在性能和质量评测证明必要后用于候选 Top-N，绝不能输入整个图片库。

### 6.2 RAG：让 LLM 基于检索证据解释

```text
Query API
  → retrieval Top 10~20
  → permission/quality filter Top 3~5
  → prompt = user question + evidence JSON
  → LLM
  → answer + evidence(objectId, objectVersion, score)
```

第一版 LLM 只读取 metadata、tag、caption 和检索结果；以后 VLM 最多读取 Top-K 缩略图。它不能通过 Prompt 获得
全库路径、直接访问 DataNode 或绕开权限。

### 6.3 Agent：最后才加入可写工具

Agent 不是“套一个聊天框”。先有只读 `search_assets` 与审计，再逐步增加受控动作：

```text
SearchTool          有 scope 的对象检索
DescribeTool        基于已授权 metadata/derivative 生成说明
CreateDatasetTool   显式创建版本快照（需用户确认/审批）
AlbumTool            创建逻辑相册（后续）
```

初期禁止 Agent 删除对象、发放 DataNode 凭据、直接改 MiniDriver Metadata。每次工具调用与结果版本必须保存为
`agent_run` evidence。

## 7. 实施阶段与当前状态

| 阶段 | 目标 | 交付与边界 |
|---|---|---|
| A | 前端搜索契约 | 标签/输入框、`asset_id = objectId`、结果回到原对象预览；已完成 Lite |
| B | 事件驱动自动索引 | Outbox → Redis Streams → durable Job → Worker；已完成 Lite |
| C | 真实多模态派生 | OpenCLIP 可选、EXIF/质量/pHash、评测；已完成 Lite，真实质量仍需更多中文评测 |
| D | 可扩展检索 | PostgreSQL + pgvector、分页、filter、查询指标；已完成基础实现 |
| E | 数据资产治理 | Dataset 成员版本快照、lineage 基础；E0 已完成 |
| F | 受控 Agent | 只读搜索 Agent、evidence/audit；F0 已完成 |
| G | GPU 生产化 MVP | 真实本地 embedding、优先级、Backfill checkpoint、GPU batch 与指标；下一主线 |
| H | Local RAG | `/ai/rag/query`、mock LLM 后接本地量化 LLM；下一主线 |
| I | VLM/Caption/Tag | Top-K caption/OCR/VLM、人工修订与模型版本；后续 |
| J | 规模平台 | 多 Worker、Kafka/独立向量库/Spark/k3s，必须由实际瓶颈驱动；后续 |

现有实现和代码状态必须以各 Phase 测试报告为准，不能把“目标阶段”写成已经交付。

## 8. RTX 4000 约 8 GiB：保守 GPU 方案

两类工作使用显存的模式不同：

```text
Embedding：离线批量吞吐，输入固定、可微批处理。
RAG generation：在线低延迟，模型权重 + KV cache，随并发和上下文增长。
```

默认不要让视觉 embedding 模型与 4B/7B LLM 同时常驻。采用单 GPU Broker 的时间隔离：

```text
RAG arrival → embedding 停止领取新 batch → 当前 batch 结束 → GPU lease 切给 LLM
RAG idle    → LLM 释放 lease → embedding 继续
```

先验证一个真实的图文 embedding 模型与中文照片集的 Recall@K，再接文本 RAG。量化 LLM、上下文长度、并发数与
GPU memory budget 都要在目标机器实测；8 GiB 不能事先承诺“两个模型一定同时可用”。

## 9. 高并发框架

```text
MiniDriver upload/download  独立的 C++ 网络与磁盘背压路径
Index jobs                  durable queue + CPU/GPU 有界 worker pool
Search API                  无状态、可横向扩展；DB 服务端 ANN
RAG API                     独立 request queue、max tokens/concurrency
```

必须监控并限制：浏览器并发文件数、pending job 数、对象读取并发和超时、CPU decode、GPU batch、LLM 并发、
重试次数和 DLQ。没有边界的“更快处理全部照片”最终会让内存、显存、网络或对象存储先崩溃。

## 10. 部署框架

第一版用 Docker Compose，而不是先上 Kubernetes：

```text
Storage hosts      MiniDriver Gateway / Metadata / DataNode
AI control host    PostgreSQL + pgvector / Redis / Query API / stream ingestor
GPU host           embedding worker / optional LLM server / GPU broker
CPU hosts (optional) preview, EXIF, thumbnail, backfill workers
```

AI 组件可以扩容，MiniDriver 仍可独立运行。Kubernetes/k3s 只在多服务发布、资源调度和故障恢复的人工成本确实超过
Compose 后引入；它不是分布式系统正确性的替代品。

## 11. 最小成功标准

```text
上传 JPEG 后原图先可靠 COMMITTED，AI 故障不影响上传/下载。
重复 Commit event / Worker retry 不生成重复 Embedding。
历史 Backfill 有 checkpoint，新上传不被其饿死。
文本查询只计算一个 query vector，向量库服务端返回 Top-K。
结果能打开固定 objectId + objectVersion，RAG 回答带同样的 evidence。
GPU 满载时系统排队/限流而非 OOM；各队列、延迟和失败可观测。
```

## 12. 与现有文档的关系

- [MiniDriver/S3 接入与回填设计](CINELAKE_MINIDRIVER_AND_S3_STORAGE_CONNECTOR_DESIGN_2026-08-25.md)：存储 Adapter 与版本/回填细节；
- [向量检索与本地 RAG MVP（RTX 4000 8 GiB）](CINELAKE_VECTOR_RAG_MVP_RTX4000_8GB_PLAN_2026-08-25.md)：当前可执行 GPU/RAG 小步计划；
- [既有 CineLake 平台架构](CINELAKE_MINIAI_PLATFORM_ARCHITECTURE_2026-08-25.md)：当前实现与平台结构补充。

本文件是总路线图：先让对象版本、事件、任务、向量和证据正确连接，再逐步增加模型、GPU 优化和大规模调度。
