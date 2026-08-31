# CineLake 与 MiniDriver/S3 对象存储的接入、索引与回填设计（2026-08-25）

> 状态：目标接口设计。  
> 项目边界：MiniDriver 是对象存储项目；CineLake 是独立部署的 AI 数据资产、向量检索与 RAG 项目。  
> 结论：CineLake 不应绑定某一种对象存储协议；当前通过 MiniDriver HTTP Adapter 接入，未来可并行支持 MinIO/S3 Adapter。

## 1. 要解决的实际问题

摄影素材库会同时出现两类工作：

```text
新上传：用户刚上传一张 JPEG / RAW，希望立即保存，不希望等待 GPU。
历史回填：已经有数万张素材，需要逐步生成缩略图、EXIF、向量和标签。
```

系统必须同时保证：

- 原图可靠保存，不被 AI Worker、GPU 故障或向量库故障影响；
- AI 可以从可靠的对象版本读取数据，但不需要了解 DataNode 磁盘目录；
- 同一对象版本的任务可重试、可恢复、不重复生成无限向量；
- MiniDriver HTTP 与 MinIO S3 的差异被隔离在适配器中；
- 后续查询只在向量数据库附近完成，不把全库向量或原图传给浏览器。

## 2. 两个项目的职责

```text
MiniDriver
  原始对象真相：JPEG / RAW / 视频的字节、Chunk、副本、对象版本、读取授权。
  对外能力：上传、下载、预览读取、对象完成事件。

CineLake
  派生数据真相：缩略图、EXIF、质量、OCR、标签、Embedding、Dataset、搜索审计。
  对外能力：索引状态、文本/图片检索、RAG/Agent、返回对象引用。
```

任何一个 CineLake 记录都只能引用 MiniDriver/S3 中的一个固定对象版本：

```text
Asset = storageSource + objectIdentity + objectVersion + processor/model outputs
```

AI 数据库不保存原图 Body，也不能通过猜测 DataNode 的本地路径访问原图。

## 3. 统一的 Storage Connector 边界

CineLake 只依赖一个逻辑接口；协议细节被各 Adapter 封装：

```text
StorageConnector
  describe(committed object version) → ObjectDescriptor
  openRead(descriptor, read capability) → streaming bytes
  verify(content hash / length) → success or failure
  optional: openDerivativeRead(...) → streaming bytes
```

统一对象描述符的最小字段：

```json
{
  "source": "minidriver-http | s3",
  "object_id": "stable logical identity",
  "object_version": 3,
  "object_key": "/travel/2026/sunset.jpg",
  "content_type": "image/jpeg",
  "size": 9437184,
  "content_hash": "sha256-or-storage-specific-identity",
  "read_locator": "opaque; never a DataNode disk path"
}
```

`object_id + object_version` 是 CineLake 的业务幂等身份；`object_key` 只是展示路径，允许被移动或重命名。对于
S3，版本身份可以由 bucket/key/versionId 组成；如果版本控制未启用，则必须由 CineLake 保存 immutable content hash
和写入版本策略，不能只相信可变 key。

## 4. 当前 MiniDriver HTTP Adapter

当前已有的接入方式不是 S3，而是 MiniDriver 专用 HTTP 合约：

```text
1. Gateway 仅在 File Commit 成功后，经 Outbox 发布 FILE_UPLOAD_COMMITTED。
2. CineLake stream-worker 消费 Redis Stream，先持久化 event 去重账本与 Job，再 XACK。
3. Worker 请求：GET /api/v2/objects/{objectId}/manifest
4. Worker 按 manifest 的副本候选，通过 DataNode Chunk HTTP 路径读取每一个 Chunk。
5. Worker 校验每个 Chunk 的长度与 SHA-256，流式写入临时文件或处理管道。
6. CPU/GPU 处理完成后写入 CineLake 的 Asset/Embedding/Derivative 表。
```

```text
MiniDriver Gateway
  → Redis Stream: minidrive:file-events
  → CineLake stream-worker
  → PostgreSQL index_jobs（持久化、幂等）
  → MiniDriver manifest
  → DataNode chunk HTTP GET
  → CineLake CPU/GPU Worker
```

当前 V2 事件中的 `objectVersion` / `metadataVersion` 固定为 `1`；V3 Metadata 完成后必须改为真实、单调的对象版本。
当前读取路径仅适用于可信内网；V3.1 前应补 Worker 身份与短期读取 Capability，不能把 DataNode 读取地址直接交给
浏览器或任意 AI 服务。

### 4.1 MiniDriver Adapter 的目标演进

不要求 MiniDriver 为了 AI 立刻实现完整 S3 协议。更小、更安全的目标接口是：

```text
GET /internal/v3/objects/{objectId}/versions/{objectVersion}/descriptor
GET /internal/v3/objects/{objectId}/versions/{objectVersion}/read
```

第二个接口可由 Gateway 验证短期 Worker Capability 后，选择健康副本并流式读取；DataNode 仍然不对公网暴露。
Adapter 的实现可以先继续使用 manifest + Chunk GET，未来再替换为上面的单一流式读取接口，而不影响 CineLake Job、
Embedding 或搜索 API。

## 5. MinIO/S3 Adapter

MinIO 提供兼容 S3 的对象 API。它也是 HTTP/HTTPS 通信，但请求遵循 S3 资源模型与 AWS Signature V4 鉴权，而不是
MiniDriver 当前的 Session/manifest/Chunk 路由协议。

```text
CineLake S3 Adapter
  HEAD bucket/key?versionId=...
  GET  bucket/key?versionId=...
  使用 SDK 或 SigV4 签名 HTTP 请求
```

对应关系如下：

| CineLake 需要的语义 | MiniDriver HTTP Adapter | MinIO/S3 Adapter |
|---|---|---|
| 稳定对象身份 | `objectId + objectVersion` | `bucket + key + versionId` |
| 小型描述信息 | Gateway manifest | HEAD Object / Object metadata |
| 读取 Body | manifest → DataNode Chunk 流 | GET Object 流 |
| 完整性验证 | Chunk SHA-256 + manifest | ETag/校验和/应用层 hash；不能假设 ETag 永远等于 MD5 |
| 对象完成通知 | Redis `FILE_UPLOAD_COMMITTED` | MinIO/S3 event notification → Queue/Webhook → CineLake ingestor |
| 访问授权 | Worker Capability（目标） | IAM/Access Key、临时凭证或预签名 URL |

因此，未来若 CineLake 使用 MinIO，不是把 MiniDriver 改造成 MinIO，也不是让所有服务改写；只增加：

```text
MinidriverHttpConnector
S3Connector
```

两者把自己的对象与事件转化成统一的 `ObjectDescriptor` 和 `IndexJob`。MiniDriver 是否在更远期增加 S3 兼容网关，是
独立的产品决定，不应阻塞 V3-Lite。

## 6. 新上传：不在上传同步路径生成向量

上传路径只负责“可靠提交对象”；它的成功响应不等待缩略图、EXIF、模型或 GPU：

```text
Browser
  → MiniDriver Gateway：预检 / Session / Route
  → DataNode：Chunk Body + 副本
  → MiniDriver：File COMMITTED
  ← Browser：上传完成

File COMMITTED
  → Outbox Event
  → Queue
  → CineLake Job = PENDING
```

随后 CineLake 异步执行有限 DAG：

```text
JPEG
  → EXIF ─────────────┐
  → thumbnail ────────┼→ Asset metadata READY（可浏览）
  → quality / pHash ──┘
  → embedding GPU job → Vector READY（可语义搜索）

RAW
  → extract preview → thumbnail / quality / embedding

video（后续）
  → probe → keyframes → embedding aggregation
```

此时用户可看到两个独立状态：

```text
Object: COMMITTED                  原文件已安全可用
AI:     PENDING / INDEXING / READY  索引是否可搜索
```

GPU 队列饱和时，图片停留在 `PENDING` 或 `GPU_QUEUED`，但 MiniDriver 的上传、下载、Metadata Raft 和 Repair 不受影响。

## 7. 历史海量数据：Backfill，不重新上传也不一次读完

已有大量素材时，应该执行有 checkpoint 的发现与回填：

```text
Catalog Scanner
  → 分页列出已 COMMITTED 对象版本
  → 对每项计算 index key：source/objectId/version/processor/model
  → 已 READY：跳过
  → 未 READY：写入 IndexJob
  → 保存 cursor/checkpoint

CPU/GPU Worker
  → 只在有 worker capacity 时领取 Job
  → 拉取一个对象或 preview
  → 处理、写结果、记录成功/失败
```

不能在启动时一次把所有对象下载到 GPU 机器；应有如下资源界限：

```text
catalog page size          有界
pending job queue          有界
CPU decode concurrency     有界
GPU embedding concurrency  有界（通常 1 个 model worker）
GPU micro-batch            16 / 32 / 64，由显存和测量决定
object read size/timeouts  有界
```

新上传与 Backfill 使用同一个 `IndexJob` 模型，但优先级不同：

```text
interactive：新上传、用户明确请求目录
background：历史全量回填、模型升级重算
```

调度器应优先处理 interactive 队列，避免旧库回填把新上传的搜索体验完全堵住。

## 8. 向量何时传输、存储与查询

### 8.1 首次生成确实需要读取图片

视觉 embedding 不可能凭 metadata 猜出图片内容。首次索引或模型升级时，CineLake 必须从对象存储读取原图或可接受的
preview：

```text
object bytes / preview
  → CPU decode + resize
  → GPU image encoder
  → normalized image embedding
  → vector store
```

这是一遍后台读取，按 Job 限速和 batch 执行；它不是上传同步操作。对于 exact duplicate，可在权限允许时按
`content_hash + modelId` 复用已有派生结果，避免重复读取和推理。

### 8.2 后续文本查询不传输全库向量或图片

```text
Browser → POST /ai/search {query: "日落山景", filters: ...}
Query API → text embedding model，生成一个 query vector
Query API → PostgreSQL + pgvector / future Vector DB 在服务端执行 ANN Top-K
Browser ← Top-K object references、缩略图 URL、标签、分数
```

向量留在 vector store；浏览器、Gateway、LLM 都不读取百万条向量。Agent 只拿到已授权的 Top-K 证据：

`objectId + objectVersion + metadata + thumbnail/preview reference + score`。

### 8.3 向量与模型版本

```text
Embedding identity = source + objectId + objectVersion + modality + modelId
```

更换 embedding 模型、对象内容变更或重新抽取 preview 时，产生新的索引任务；旧向量不能无条件与新模型向量混排。
查询时必须指定/选择一个兼容 `modelId` 的索引集合。

## 9. 最小数据模型

```text
assets
  asset_id / source / object_id / object_version / object_key
  content_type / content_hash / index_state / metadata_json / updated_at

derivatives
  asset_id / object_version / derivative_type / processor_version
  state / location / checksum / created_at

embeddings
  asset_id / object_version / modality / model_id
  vector / created_at

index_jobs
  job_id / idempotency_key / priority / state / attempts
  retry_at / last_error / source_event_id
```

`index_jobs.idempotency_key` 例如：

```text
minidriver/object-123/v3/image-embedding/openclip-v1
```

Redis/Kafka 是任务分发媒介，不是最终任务真相；任务状态、版本和幂等结果需要存入 PostgreSQL。

## 10. 可靠性与删除边界

```text
MiniDriver/S3 commit
  → Durable Outbox/Event
  → CineLake persist Job
  → ACK queue
  → process/retry
```

事件语义为至少一次投递，因此 CineLake 以 `eventId` 去重，以对象版本任务键合并重复任务。索引失败不能反向删除或
损坏原对象。

后续必须增加：

- `OBJECT_DELETED` / tombstone 事件，令 CineLake 停止读取并隐藏对应 Asset；
- Worker 短期读 Capability 或 S3 临时凭证，避免常驻明文密钥；
- 用户/tenant scope 注入 Search 与 Object Read，不能让 AI 返回越权对象；
- 原对象和派生对象的生命周期/垃圾回收协议。

## 11. 实施顺序

```text
Step 1（当前）
  固定 MiniDriver HTTP + Redis Stream Adapter 的对象/事件契约。

Step 2
  V3 Metadata 提供真实 objectVersion；Outbox 绑定已提交 Metadata 状态。

Step 3
  CineLake Job 优先级、CPU/GPU 队列、Backfill scanner 与 checkpoint。

Step 4
  以 PostgreSQL + pgvector 保存版本化 embedding；前端只调用 Query API。

Step 5
  增加 S3Connector，使用 MinIO 做第二种对象存储来源的集成测试。

Step 6（按规模决定）
  将 Redis Streams 扩展/替换为 Kafka，或将 pgvector 演进为独立向量库；
  这是实现指标驱动的替换，不是第一版前置条件。
```

## 12. 当前最小验收链路

```text
1. 浏览器上传 JPEG 到 MiniDriver。
2. File Commit 成功，Gateway Outbox 最终发布 FILE_UPLOAD_COMMITTED。
3. CineLake stream-worker 写入一条幂等 IndexJob 后 ACK。
4. Worker 从 manifest/DataNode HTTP 流式读取并校验对象。
5. CPU 写缩略图/EXIF；GPU（或当前模型 provider）写 embedding。
6. pgvector 搜索返回 objectId + objectVersion。
7. 前端通过 MiniDriver Gateway 打开原对象或受控预览。
```

这条链通过后，才开始历史 Backfill、模型升级重算、S3/MinIO 第二来源与 RAG；不要让 S3 兼容、Spark 或 K8s 抢占
V3-Lite Metadata HA 与最小索引闭环的开发顺序。
