# CineLake AI 功能：从最小检索闭环到多模态数据资产平台（2026-08-20）

## 1. 一句话目标

MiniDrive 已解决“摄影素材如何可靠地上传、存储和下载”。CineLake AI 要解决的是：

> 让已经安全存储的图片、RAW、视频和文档，变成可被发现、筛选、理解和复用的数据资产。

最终用户不必只记得文件夹和文件名，而可以用自然语言和结构化条件找到素材：

```text
找 2025 年用 Nikon 拍摄的、带有寺庙和暖色夕阳的照片
```

系统返回候选素材、相机/时间/地点等元数据，并能打开 MiniDrive 中的原对象。这里的重点不是做一个
“聊天机器人”，而是建立一条从对象存储到多模态索引的基础设施链路。

## 2. 为什么值得做

摄影、视频与训练数据有相同的问题：文件本身很大，但真正困难的是“之后还能不能找到、筛选和复用它”。

| 只有对象存储时 | 加入 AI 数据资产能力后 |
|---|---|
| 通过目录/文件名寻找文件 | 通过文字、标签、时间、相机等组合检索 |
| 上传后只有字节与基础元数据 | 上传后异步提取 EXIF、质量、场景和向量 |
| 文件是孤立对象 | 文件属于 Dataset，可记录来源、版本和处理状态 |
| 业务端重复读文件做分析 | 后台统一生成可复用的派生数据与索引 |

对 MiniDrive 而言，这条方向将已有的 C++ 网络、对象存储、异步日志和媒体处理基础，延伸到：

```text
多模态对象存储 + 事件流水线 + 元数据治理 + Embedding + 向量检索
```

这比单独做一个 RAG 问答页面更能体现数据基础设施项目的连续性。

## 3. 最终用户闭环

最终体验应当是一条可解释的闭环：

```text
1. 用户上传 RAW / JPEG / 视频
2. MiniDrive 可靠写入双副本并完成 File Commit
3. 后台自动索引，不阻塞上传完成响应
4. 用户在工作台搜索「雾中的山景」或筛选「Nikon + 2025」
5. 返回已索引素材、标签、质量和相似度
6. 用户打开原图、下载、选中多个素材或生成说明
```

其中“可靠上传”仍由 MiniDrive 数据面负责；“理解、索引和检索”由 CineLake AI 负责。两者解耦，但通过
`objectId`、对象版本和完成事件建立可靠连接。

## 4. 当前最小单元：已实现什么

当前实现刻意保持小，目的是先验证完整数据契约和用户交互，而不是假装已经具备大数据平台能力。

### 4.1 前端最小单元

`www-v2` 已发布的 `v2.0.1` 增加了 `AI FIND / 02.1`：

```text
用户输入自然语言 / 点击标签
  → 标签合并为查询文本
  → POST AI-App-Lite /ai/search
  → 展示 object_key、元数据摘要、相似度
  → asset_id 等于当前目录 objectId 时，复用 MiniDrive 原预览器
```

初始标签：日落、寺庙、城市夜景、人像、山景、鸟类。

该界面已经处理了：异步请求状态、搜索竞态、服务不可用提示与跨域调用；它没有自己读取对象字节，也没有
改变 Gateway/DataNode 上传路径。

### 4.2 AI-App-Lite 最小单元

`ai-app-lite/` 是当前 V3 工作区内的独立 Python 原型：

```text
POST /ai/assets
  → SQLite jobs(pending)
  → Worker claim
  → 基础元数据 / 可选 EXIF / 文件 hash
  → Embedding Provider
  → SQLite assets(ready)

POST /ai/search
  → 文本 Embedding
  → metadata filter
  → cosine Top-K
  → 返回 asset_id、object_key、metadata、score
```

已提供：

- SQLite Asset 表和 Job 表；
- `pending → processing → done/failed` 异步任务状态；
- 可选 EXIF 提取；
- 余弦相似度的向量 Top-K；
- 元数据过滤；
- CORS 预检支持；
- 可选 OpenAI-compatible 文案接口；
- 无模型依赖的 `hash-smoke` provider，以及可选 OpenCLIP Provider。

### 4.3 当前最重要的真实性边界

当前 MVP 还不是“上传照片后立刻可以真正语义搜索”的完整产品，原因如下：

1. `hash-smoke` 只验证管道，不能理解图像或文字语义；
2. `POST /ai/assets` 仍保留作本地调试入口，但正常上传已不需要手工登记；
3. V2 Gateway 已实现 `File Commit → LevelDB Outbox → Redis Stream`，但 V2 的
   `objectVersion/metadataVersion` 目前均为 `1`；
4. Worker 已通过 manifest 和 DataNode Chunk HTTP 路径读取对象并校验每个 Chunk SHA-256；当前路径仅适合
   可信内网，尚未加入生产级 Worker 身份认证、短期读令牌和下载配额；
5. SQLite 顺序扫描、失败任务恢复与 DLQ 只适合小规模演示。

因此当前最准确的表述是：

> 已完成“上传 Commit—可靠事件发布—幂等入队—对象读取—异步索引—向量查询—前端展示”的最小闭环；
> 真实图文语义、生产级访问控制和大规模向量检索仍是下一阶段工作。

## 5. 核心数据契约

对象存储和 AI 系统必须共享稳定身份，而不是只靠文件名匹配：

```text
MiniDrive objectId  =  AI asset_id
```

建议的索引事件为：

```json
{
  "event_type": "FILE_UPLOAD_COMMITTED",
  "event_id": "uuid",
  "object_id": "MiniDrive objectId",
  "object_key": "/travel/2025/temple-sunset.jpg",
  "file_hash": "sha256...",
  "size": 523424,
  "content_type": "image/jpeg",
  "object_version": 1,
  "metadata_version": 1,
  "occurred_at": "2026-08-20T00:00:00Z"
}
```

这个契约解决几个实际问题：

- `event_id`：消息重复投递时可去重；
- `object_id`：同一对象不会生成多份资产记录；
- `file_hash` / `object_version`：内容变更时能决定是否重建向量；
- `metadata_version`：EXIF、标签等元数据变化时能单独重建索引；
- `object_key`：给前端展示和目录跳转，不作为唯一身份。

## 6. 最终目标架构

成熟形态不要求一次实现完，但各层职责应从开始就清楚：

```text
                           User / AI Agent
                                  │
                         Web Search / Query API
                           │              │
                    Metadata Filter    Vector Retrieval
                           │              │
                    Catalog / SQL     pgvector / Milvus
                           │              │
=========================== CineLake Query Layer ============================

MiniDrive File Commit
        │
        ▼
Event Bus (Redis Streams → Kafka, if scale requires)
        │
        ▼
Index / Processing Workers
  ├─ EXIF / OCR / 视频抽帧
  ├─ 质量评估 / 去重
  ├─ OpenCLIP Embedding（batch/GPU 可选）
  └─ Metadata / Vector Upsert
        │
        ▼
Metadata Catalog + Dataset / Version / Lineage
        │
=========================== CineLake Data Asset Layer =======================
        │
MiniDrive Object Storage
  Gateway metadata + DataNode replicated object bytes
```

几个关键原则：

- Event Bus 只传对象身份和小型元数据，绝不把 RAW/视频 Body 放进消息；
- Worker 通过受控对象读取接口获取已 Commit 的对象；
- 索引失败不影响原对象可下载，但 Asset 状态应可见、可重试；
- 向量、缩略图、OCR 等都是对象的派生数据，应带版本和来源；
- GPU/模型推理属于可扩容的 Worker，不应侵入 Gateway 的低延迟控制路径。

## 7. 演进路线

### Milestone A：界面与检索契约（当前完成）

```text
目标：验证用户能使用标签/文本触发检索，前端能展示并回到对象预览。
```

- `www-v2` 标签、搜索输入、结果卡片；
- AI API / Worker / SQLite Job Queue；
- `asset_id = objectId` 契约；
- Hash smoke provider 和 CORS 验证。

验收：手工登记一条素材后，任务成功、搜索 API 返回结果、当前目录中可打开原对象。

### Milestone B：对象 Commit 到事件驱动索引（Phase B Lite 已完成）

```text
Gateway File Commit
  → Redis Stream FILE_UPLOAD_COMMITTED
  → AI Worker 消费并幂等索引
```

已实现：

- Gateway 将 Object Commit 与 `ai:<eventId>` outbox 写入同一个 LevelDB `WriteBatch`；仅成功 Commit 后才由异步
  Publisher `XADD FILE_UPLOAD_COMMITTED`，发布成功才标记 outbox 为已发布；
- Redis 消费者组先把 `eventId` 去重账本、`objectId + objectVersion` 目标键和 SQLite Job 持久化，再 `XACK`；
  Redis 7 使用 `XAUTOCLAIM`，Redis 5/6 回退为 `XPENDING + XCLAIM` 回收超时 pending 消息；
- Worker 按 `objectId` 获取 manifest，从副本候选读取 Chunk，并逐 Chunk 校验长度和 SHA-256；
- SQLite Job 已有 `pending → processing → done/failed`，Asset 只在索引成功后成为 `ready`；
- C++ outbox 回归、Redis C++ Publisher + Python consumer 联调、重复事件和 Worker bridge 测试已覆盖。

验收边界：部署 Gateway、Redis、`stream-worker` 和 `worker` 后，从网页上传 JPEG，无需手工调用
`/ai/assets`；任务完成即可出现在搜索中。真实 OpenCLIP 需显式切换 Provider，默认 hash provider 只验证链路。

### Milestone C：真实多模态检索与元数据增强（Phase C Lite 已完成）

```text
目标：让“日落寺庙”“鸟类”等文字查询真的具备图文语义。
```

- 接入可选 OpenCLIP（默认 `ViT-B-32-quickgelu/openai`），并将 `embedding_model` 作为向量/查询兼容性契约；
- 实现有界批量图片编码和 zero-shot 场景标签；
- 提取 EXIF、Pillow 启发式亮度/对比度/锐度/曝光/质量分数与 perceptual hash；
- 增加固定 case 的 Recall@K/MRR 评测命令和模型隔离测试；
- RAW、视频、OCR 与 GPU 多进程调度未在 Lite 中实现：没有 preview/keyframe 时显式失败，后续以派生 DAG 增加。

验收边界：自动化测试使用语义 Stub 验证批处理、标签、模型版本和 Recall 计算；真实模型已在 56 张本地摄影
测试集上完成一次 CPU 基线，但不能外推为通用质量或生产吞吐。详见
[Phase C 架构与测试报告](CINELAKE_PHASE_C_SEMANTIC_INDEX_ARCHITECTURE_AND_TEST_REPORT_2026-08-21.md)。

### Milestone D：可扩展检索服务（Lite fallback + PostgreSQL 已验收）

```text
目标：从 SQLite 演示升级为可并发查询的数据服务。
```

- 已实现跨目录结果、keyset 分页、稳定排序、metadata filter、查询耗时/命中数/索引新鲜度观测；
- 当前使用 SQLite 真源 + 本进程 ready-asset snapshot，显式返回缩略图 `unavailable` 状态；
- PostgreSQL/pgvector Repository、Compose 和 migration 已实现并通过真实 Docker 集成测试；
  Lite 缓存仅是无依赖 fallback，不能称为多进程可扩展向量库；
- 规模/召回要求明确后，再比较 pgvector、Milvus 或 Faiss/HNSW。

验收：真实 56 张摄影集已给出 SQLite query-executor P50/P95 与 1/4 并发结果；PostgreSQL 路径的验收命令与
边界见 [PostgreSQL + pgvector 实施说明](CINELAKE_PHASE_D_POSTGRES_PGVECTOR_IMPLEMENTATION_2026-08-21.md)。

### Milestone E：数据资产与治理（E0 最小闭环已完成）

```text
目标：从“搜索素材”提升为“管理可复用的数据集”。
```

- 已引入 Dataset、成员关系、owner/purpose 与对象版本快照：成员记录 `(asset_id, object_version)`，后续
  重建同一 Asset 不会篡改历史 Dataset；
- 已在 SQLite 与 PostgreSQL/pgvector 后端实现 Dataset 表和成员表，提供创建、列举、查询和显式选入 API；
- 访问控制、标签人工修订、数据质量规则、生命周期/成本统计仍未实现；当前 `owner` 是记录字段而非身份认证；
- 在批量分析需求出现时，用 Iceberg 管理结构化元数据快照，并由 Spark/Flink 承担批流计算。

Iceberg、Spark、Flink 不是 MVP 必需品；它们解决的是大量历史数据的表格式版本、批处理和流处理问题，应在
数据规模和实际任务明确后接入。

### Milestone F：面向 AI 的上层服务（F0 只读检索已完成）

```text
目标：让模型或 Agent 使用经过治理的素材，而不是直接堆一个聊天页面。
```

- 已提供只读 `POST /ai/agent/search`：复用向量检索，返回对象版本证据，并把 caller/query/filter/模型/排序结果写入
  Agent Run 审计账本；
- F0 Agent 没有删除对象、改写 Asset、创建 Dataset 或训练集导出的工具权限；
- RAG 的真实调用者认证、Dataset scope 权限过滤、受审批的 `create_dataset` 工具以及训练样本导出仍是后续工作。

E0/F0 的架构、接口和测试证据见
[Dataset 治理与只读 Agent 最小实现报告](CINELAKE_PHASE_E_F_MINIMUM_GOVERNED_AGENT_IMPLEMENTATION_2026-08-21.md)。

## 8. 各阶段的实现设计细节

本节是实际开发时的设计约束。标注“当前”的内容已经有最小实现；标注“目标”的内容是后续阶段设计，
不能在简历或报告中提前宣称完成。

### 8.1 Milestone A：前端与 AI-App-Lite 的最小闭环（当前）

#### 模块边界

```text
www-v2/app.js
  负责：标签、查询输入、异步 fetch、展示、打开既有预览器
  不负责：读取对象 Body、生成向量、保存资产元数据

ai-app-lite/server.py
  负责：HTTP 校验、入队、检索、返回 JSON/CORS
  不负责：在 HTTP Handler 内同步做 EXIF 或模型推理

ai-app-lite/worker.py
  负责：后台领取任务、读取本地素材、生成派生元数据与向量
```

#### 前端查询状态机

```text
Idle
  ├─ 点击标签 → TagsSelected（更新 input，不发请求）
  ├─ 提交空查询 → InputError
  └─ 提交有效查询 → Loading

Loading
  ├─ 200 + results → Ready
  ├─ 网络/HTTP 错误 → ServiceUnavailable
  └─ 用户发起更新的请求 → 忽略旧 response，保持新请求的 Loading
```

实现中 `requestId` 递增。请求 A 尚未返回时用户提交 B，A 的响应即使后来到达也不能重绘页面。这比取消
网络请求更重要，因为网络请求可能已经在服务端执行；前端只需保证 UI 最终反映最新意图。

#### 当前 SQLite 数据模型

```text
jobs
  job_id / kind / payload_json / status / attempts / last_error
  created_at / updated_at

assets
  asset_id / object_key / local_path / metadata_json
  embedding_json / embedding_provider / status / updated_at
```

`jobs` 描述“还有什么工作需要做”，`assets` 描述“当前可供查询的索引结果”。二者分开可以避免未完成的
任务被搜索服务错误当作 READY 资产。

任务认领按以下原则进行：

```text
SELECT 最早 pending Job
  → 条件 UPDATE ... WHERE status='pending'
  → 更新成功才成为 processing Worker
```

当前设计适合单机 SQLite/少量 Worker。Worker 异常会将 Job 留在 `processing`，因此下一阶段必须增加
lease/可见性超时或显式恢复扫描；这正是 Redis Streams consumer group 要解决的问题之一。

#### 当前查询算法

```text
query text
  → embed_text(query)
  → 逐项读取 ready assets
  → metadata filter
  → cosine(queryVector, assetVector)
  → score 降序，截取 Top-K
```

向量维度、向量 Provider 与资产索引必须匹配。换模型时不能把新查询向量与旧模型的资产向量混排；正式实现
应在 `assets` 中保存 `embedding_model`/`embedding_version`，并在检索时只查询兼容版本或触发重建。

#### 当前接口时序

```text
登记：
Caller → POST /ai/assets → API 写入 jobs(pending) → 202 + job_id
Worker → claim → 生成 metadata/vector → upsert assets → jobs(done)

检索：
Browser → POST /ai/search(query) → API embed_text → AssetStore.search
        ← {asset_id, object_key, metadata, score}
Browser → 若 asset_id 在当前 Catalog 中 → selectObject → 原 MiniDrive 预览路径
```

### 8.2 Milestone B：Commit 事件与自动索引（Phase B Lite 实现）

#### 为什么需要 Outbox，而不是在 Handler 中直接 `XADD`

若 Gateway 已把 File Commit 写入自己的元数据，但进程在发布 Redis 事件前崩溃，就会产生“对象存在、却永远
没被索引”的遗漏。反过来，先发事件再 Commit，又可能让 Worker 索引一个最终失败的对象。

Phase B Lite 已采用**提交记录与待发布事件一起持久化**的 Outbox 思路：

```text
Gateway File Commit
  → 原子地写入 ObjectRecord(COMMITTED) + OutboxEvent(PENDING)
  → Commit 成功返回客户端
  → Publisher 扫描 PENDING Outbox
  → Redis XADD FILE_UPLOAD_COMMITTED
  → 成功后标记 OutboxEvent(PUBLISHED)
```

V2 的原子边界是同一个 LevelDB `WriteBatch`；Gateway 进程崩溃前未标记 `PUBLISHED` 的事件会在启动和定时
扫描中重新投递。因此语义是**至少一次发布**，消费者必须幂等。V3 接入一致性元数据后，Outbox 应成为
Metadata StateMachine 命令的结果，而不是不同 Gateway 各自的本地记录。

#### Redis Streams 设计

```text
stream: minidrive:file-events
group:  cinedata-indexers
entry:  event_id, object_id, object_version, object_key, file_hash, content_type
```

当前消费者流程：

```text
XREADGROUP / reclaim pending
  → SQLite transaction: event_id ledger + object_id/version target + IndexTask
  → XACK（只确认已持久化的入队）
  → 独立 AssetWorker 读取已 Commit 对象
  → 全部派生数据与 Asset upsert
  → Job done / failed
```

这里的 ACK 边界是“已可靠入 SQLite Job”，而不是“模型索引已经完成”：这样 Redis 消费者不会被长模型任务
阻塞，SQLite Job 承接后续 `pending/processing/done/failed` 状态。消费者在 SQLite 事务失败时不 ACK，消息会
保持 pending 并被 reclaim；模型处理失败目前记录为 Job `failed`，DLQ、退避重试和运维页面仍是后续工作。

#### Worker 对象读取接口

Worker 不直接猜 DataNode 磁盘文件路径，也不复用浏览器预览逻辑。当前实现先使用受限的 manifest 读取器：

```text
Gateway GET /api/v2/objects/{objectId}/manifest
  → Worker 读取副本候选 GET /v2/chunks/{chunkHash}
  → 验证每个 Chunk 的长度与 SHA-256
  → 临时文件交给 EXIF/Embedding，完成后删除
```

读取器限制最大对象大小和网络超时；索引数据库只保留 `minidrive://{objectId}` 逻辑地址，绝不保存已删除的
临时文件路径。生产目标仍应替换为带 Worker service credential 的内部接口/短期路由，并设置 DataNode 下载
配额和并发上限，避免 AI Worker 绕过存储系统的资源治理。

#### 自动索引状态

建议 Asset 与任务状态分离：

```text
Object: COMMITTED（对象可下载）
Asset:  PENDING → INDEXING → READY
                     └────→ FAILED（可重试 / 可查看错误）
```

对象是否可用不依赖 AI 索引；索引失败也不能让已经提交的摄影原件变成不可下载。

### 8.3 Milestone C：真实多模态处理（Phase C Lite 实现与后续 DAG）

当前 Lite 已实现 OpenCLIP Provider、`embedding_model` 隔离、有界图片 batch、zero-shot 标签、EXIF 和 Pillow
质量/pHash 元数据；`CINELAKE_PHASE_C_SEMANTIC_INDEX_ARCHITECTURE_AND_TEST_REPORT_2026-08-21.md` 记录了架构、
测试和真实模型 CPU 基线的边界。以下 DAG 是 RAW/视频/PDF 等后续处理的目标形态，不是当前已交付能力。

#### 类型路由与 DAG

一个对象不应由单一“大 Worker”按固定顺序处理，而应按内容类型展开有限 DAG：

```text
image/jpeg
  → EXIF → thumbnail → quality → OpenCLIP image embedding

image/raw
  → EXIF → preview extraction → quality → OpenCLIP(preview)

video/*
  → probe → keyframes → per-frame embedding → aggregation

application/pdf
  → text extraction / OCR → text chunks → text embedding
```

每一个派生任务至少携带：`object_id`、`object_version`、`derivative_type`、`processor_version`。
这四个字段构成派生结果的幂等键：同一版本、同一处理器不重复跑；处理器版本升级时可有计划地重算。

#### OpenCLIP 接入方式

对真实图文检索，Provider 应升级为清晰的接口：

```text
embed_image(image bytes) → normalized float[d]
embed_text(text)         → normalized float[d]
model_id()               → "ViT-B-32/laion2b..."
```

- `normalized`：先将向量归一化，使点积可直接等价于余弦排序；
- `model_id`：防止不同模型向量混用；
- batch：GPU/CPU 推理按批读取多张图，而不是每个 Event 单独加载模型；
- backpressure：推理队列必须有上限，饱和时延迟消费消息而不是无限占用内存。

质量、OCR 或自动标签结果必须被记录为模型输出及版本，而非人工事实。例如 `scene=temple` 应带
`source=model`、`model_version`、`confidence`，允许后续人工校正。

#### 去重策略

分两层处理：

```text
精确去重：file_hash 相同 → 复用已有对象/索引结果（需考虑权限）
近似去重：perceptual hash 或向量近邻 → 只标记候选，不自动删除
```

近似图片不能仅因向量接近就直接删除，连拍、不同裁剪和相同场景都可能相关但不等价。

### 8.4 Milestone D：可扩展查询服务（Lite 当前，PostgreSQL 为目标）

#### 迁移后的逻辑模型

```text
objects(object_id, object_version, object_key, owner, state, ...)
assets(object_id, object_version, index_state, metadata_json, ...)
embeddings(object_id, object_version, modality, model_id, vector, created_at)
derivatives(object_id, object_version, type, processor_version, location, state)
```

PostgreSQL 负责事务性元数据、过滤条件和分页；pgvector/向量库负责近邻候选。二者不是替代关系：先用
结构化条件缩小可见范围，再做向量召回，最后可以按时间、质量、权限或 rerank 分数综合排序。

当前 Lite 使用相同的逻辑边界，但 SQLite 负责持久化，并在单个 AI API 进程中缓存不可变的 ready Asset
快照。查询排序是 `score DESC, asset_id ASC`，cursor 绑定 vector/filter/model/snapshot；`upsert_asset`
成功后该进程内的快照立即失效。它证明了 API 契约和分页语义，不能解决多进程缓存一致性或大规模 ANN。

#### 查询执行计划

```text
Query(text, filters, caller identity)
  → 鉴权并得到可访问 Dataset/Object 范围
  → 文本 embedding
  → metadata pre-filter（时间、相机、目录、Dataset）
  → vector Top-N retrieval
  → 可选 cross-encoder / business rerank
  → 补齐缩略图、对象状态、来源、版本
  → 返回稳定分页游标
```

最初不需要自研 ANN：数据量中等时优先使用 pgvector；只有当向量规模、延迟和过滤组合证明其不足，再评估
Milvus/Faiss/HNSW 参数。评估必须记录 Recall@K 与 P95，而不能只比较单次查询速度。

#### 前端结果设计

跨目录搜索后，结果卡片应增加：缩略图、虚拟路径、Dataset、拍摄时间、模型/索引版本和“打开原对象”路由。
前端不能依赖“对象一定处于当前目录”；应请求一个授权的 `GET /objects/{objectId}` 或返回可打开的 object
route，再复用既有预览器。

### 8.5 Milestone E：Dataset、版本与治理（E0 当前 + 后续目标）

#### Dataset 模型

```text
Dataset
  dataset_id / name / owner / purpose / access_policy / created_at

DatasetMember
  dataset_id / object_id / object_version / role / added_at

Lineage
  output_object_or_derivative / input_object_ids / processor_version / run_id
```

Dataset 不是一个普通目录：目录解决人类浏览，Dataset 解决“哪些版本的哪些对象用于某次训练、标注或导出”。
成员关系必须记录对象版本，避免对象被覆盖后历史训练集悄悄改变。

E0 当前已将 `Dataset` 和 `DatasetMember` 落入 SQLite/PostgreSQL：创建 Dataset 时记录 `name/owner/purpose`；
将 Ready Asset 加入成员时读取其当前 `metadata.object_version` 并写成不可变快照。同一个 Asset 之后变为 v2 时，
旧成员仍引用 v1；若要使用 v2，必须显式再加入一次。E0 没有用户认证、对象删除 tombstone、人工标签修改或完整
Worker lineage，不能宣称完成企业级治理。

#### 版本与重建

以下变化都应可判断是否需要重算：

```text
对象内容变化          → object_version 增加，全部派生结果失效
模型升级              → embedding model_version 变化，只重建向量
EXIF/标签人工修订     → metadata_version 增加，不一定重跑视觉模型
缩略图处理器升级      → derivative processor_version 变化，只重建缩略图
```

治理的第一版先做到“可追溯”，再讨论复杂权限或成本核算：谁上传、来自哪个对象版本、由哪个 Worker/模型生成、
何时失败或重试，都应能查询。

#### Iceberg/Spark/Flink 的正确接入时机

当 Dataset 元数据、处理日志和质量结果已经需要大规模批量扫描、快照与 Schema 演进时，再将这些**结构化记录**
同步到 Iceberg 表，由 Spark 进行离线分析或由 Flink 处理持续事件。对象原始字节仍驻留 MiniDrive；不要将
所有图片 Body 复制进湖仓表。

### 8.6 Milestone F：RAG/Agent 的受控上层（F0 当前 + 后续目标）

RAG 应建立在已治理的检索服务上，而不是绕开 Catalog 直接把文件塞入 Prompt：

```text
Agent tool: search_assets(query, filters)
  → 返回 objectId、版本、来源、权限范围、摘要/缩略图
Agent tool: create_dataset(selection)
  → 写入明确的 DatasetMember 版本集合
Agent tool: describe_selection(asset_ids)
  → 仅对用户有权限的元数据/派生结果生成文案
```

每次 Agent 调用应记录 request、返回对象版本和调用者身份。这样模型的答案或训练集选择才可以复现与审计；
“模型说找到了某张图”必须能落回一个具体、可访问的 MiniDrive 对象。

F0 当前只开放 `search_assets` 等价的 HTTP 接口：`POST /ai/agent/search` 产生一个 `agent_run`，保存 caller、
query、filter、embedding model，以及按 rank 保存的 `(asset_id, object_version, score)` 证据。它用模板说明结果，
不依赖 LLM，也不拥有任何会改写对象或 Dataset 的工具。caller 目前是请求字段，尚不是可信身份；因此 F0 的审计
用于验证可追溯链路，不能替代认证/授权。

## 9. 测试与观测设计

每阶段实现都要先定义它的证据：

| 阶段 | 关键测试 | 必要指标 |
|---|---|---|
| A | 入队、Worker 成功/失败、搜索 API、CORS | Job 成功率、API 错误率、响应时间 |
| B | 重复事件、Worker 崩溃、pending reclaim、对象读取失败 | 事件滞后、待处理数、重试次数、索引新鲜度 |
| C | 固定图片/文本集的相关性、模型升级重建 | Recall@K、人工相关率、每对象推理耗时、GPU/CPU/RSS |
| D | 并发搜索、过滤组合、跨目录权限 | P50/P95 查询时延、Top-K Recall、索引大小 |
| E | Dataset 成员版本快照、重建后历史 Dataset 不变、SQLite/pgvector 一致性 | 版本快照正确率、可追溯成员覆盖率 |
| F | 只读 Agent 引用版本正确性、Run 审计、无 Asset 变更 | 引用可复现率、越权写入数（应为 0） |

最重要的端到端检查是：成功上传对象后，最终下载内容 SHA-256 仍与上传内容一致；AI 索引只能读取和派生，
绝不能改变 MiniDrive 中的原对象。

## 10. 当前最合适的下一件事

Phase C Lite 已补上模型接口、批处理和评测工具，并完成一次 56 张摄影集的真实 CPU 基线。下一步不应立刻堆
Kafka、Spark 或 Milvus，而应先扩大真实检索质量与任务可靠性的证据：

```text
优先 A：扩充固定图片/查询集与人工相关性标注，比较 prompt、模型和 batch 对 Recall@K/耗时的影响
优先 B：为 failed Job 增加退避重试、处理 lease 和可见的失败列表
V3 联动：将 objectVersion/metadataVersion 与 Gateway 的一致性元数据接通
```

OpenCLIP、pgvector、Dataset 和 Iceberg 都可建立在现有事件边界上逐步增加；只有数据规模和实际负载明确后，
才需要 Kafka/Spark/Flink 等更重的组件。

## 11. 不应过早做的事情

以下功能听起来完整，但现在实现会掩盖真正的主线：

- 直接把 Spark、Flink、Kafka、Kubernetes 全部部署起来；
- 先做通用 RAG 聊天机器人；
- 把 AI 推理同步放到 Gateway 上传请求中；
- 让消息队列传输图片/视频 Body；
- 在没有真实模型评估前宣称“语义搜索效果很好”；
- 在没有对象版本/幂等约束前做自动重试和批量处理。

当前应始终遵循：先建立正确的对象身份与事件边界，再扩展模型、数据规模和平台组件。
