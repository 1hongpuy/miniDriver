# CineLake（miniAI）：多模态数据资产与 RAG 平台总架构（2026-08-25）

> 项目名：**CineLake**；`miniAI` 是其面向 MiniDriver 的学习型实现代号。  
> 定位：独立部署的多模态索引、搜索和 RAG 平台；MiniDriver 是第一个接入的对象存储来源。  
> 当前实现：`ai-app-lite/` 已验证 MiniDriver HTTP + Redis Streams + PostgreSQL/pgvector 的最小闭环。  
> 非目标：本项目不替代 MiniDriver/MinIO 作为原始对象存储，也不在第一版实现 Spark、Kafka、Kubernetes 或自定义 CUDA Kernel。

## 1. 一句话目标

把“存着大量照片、RAW、视频却找不到”的对象库，变成可被结构化筛选、语义检索和 RAG/Agent 使用的数据资产库：

```text
可靠对象存储
  → 可靠事件
  → 异步派生数据
  → 版本化元数据与向量
  → 搜索 / RAG / 业务应用
```

用户最终可以提出：

```text
找 2025 年 Nikon 拍摄的暖色寺庙照片，并推荐三张适合旅行纪录片封面的素材。
```

系统要返回有版本、有来源的对象引用，而不是无法追溯的模型幻觉。

## 2. MiniDriver、MinIO 与 CineLake 的差别

### 2.1 MiniDriver 与 MinIO 不是“Web 与非 Web”的差别

两者都可以通过 HTTP/HTTPS 被 Web 应用或后端业务访问：

```text
MinIO
  通用对象存储服务器；向客户端暴露成熟的 S3 API。
  应用通常通过 S3 SDK、AWS Signature V4、预签名 URL、bucket/key/versionId 访问。

MiniDriver
  本项目自研的对象存储学习系统；当前暴露专用 HTTP API。
  客户端通过 Gateway Session、Chunk Route、Upload Capability、manifest 访问。
```

MinIO 的优势是 S3 生态、SDK、兼容性、桶/对象/版本/策略等产品完备性；它的标准 S3 endpoint 本身也是 HTTP 服务，
支持 `PUT/GET/HEAD`、multipart upload、版本和通知等对象操作。MiniDriver 的价值不是假装比 MinIO 完备，而是你能
掌握和验证其中的实现问题：客户端直传、链式副本、背压、元数据 Raft、fencing 和 Repair。

因此正确比较是：

| 维度 | MiniDriver | MinIO/S3 |
|---|---|---|
| 主要目的 | 学习并实现高性能/高可用对象存储核心机制 | 通用、标准化对象存储产品 |
| 客户端协议 | 当前自定义 HTTP | S3 HTTP API + SigV4 |
| 大文件上传 | Session + Chunk Route + DataNode 直传 | Multipart Upload |
| 元数据控制面 | V3-Lite 自建 Raft 学习实现 | 产品内部实现，对应用透明 |
| 生态兼容性 | 当前仅本项目客户端/Adapter | 各语言 S3 SDK、数据工具广泛支持 |
| CineLake 接入 | `MinidriverHttpConnector` | `S3Connector` |

MinIO S3 API 文档明确列出了对象 `GetObject`、`HeadObject`、multipart 和版本操作；客户端仍是通过网络 endpoint 使用它，
并非“不面向 Web”。[MinIO S3 API 参考](https://docs.min.io/aistor/reference/aistor-server/http-endpoints/)。

### 2.2 CineLake 的位置

```text
MiniDriver / MinIO
  = 原始字节与对象版本的真相

CineLake
  = 原始对象的派生理解层
  = 缩略图、EXIF、标签、向量、Dataset、搜索和 RAG
```

CineLake 不能持有“唯一原图”；它只保存对象引用和派生结果。即使 CineLake 故障，原图仍应可上传、下载和预览。

## 3. 平台总体架构

```text
                              Business / Browser
                                │         │
                 upload/download│         │search/RAG
                                ▼         ▼
                   MiniDriver Gateway   CineLake Query API
                         │                    │
                         ▼                    ├─ SQL metadata filter
              MiniDriver DataNodes            ├─ Vector Top-K retrieval
                raw JPEG/RAW/video            └─ optional LLM answer
                         │                    │
                    Object COMMITTED           ▼
                         │             PostgreSQL + pgvector
                         ▼                    │
                 Event Source Adapter          │
              Redis Streams / future S3 event  │
                         │                    │
                         ▼                    │
                  CineLake Ingestor ───────────┘
                         │
                         ▼
                    Durable IndexJob
                         │
           ┌─────────────┼────────────────┐
           ▼             ▼                ▼
      CPU workers    GPU workers      Backfill scanner
      EXIF/thumb     embedding/VLM    history catalog
```

每层的故障不会反向破坏上一层：

```text
GPU Worker 挂掉     → IndexJob 重试，原图仍在
pgvector 不可用      → 搜索降级，原图仍可读
Redis 重复投递       → Job 幂等，向量不重复写
MiniDriver 未 Commit → 不创建索引任务
```

## 4. 模块与数据所有权

| 模块 | 负责 | 持久化真相 | 不负责 |
|---|---|---|---|
| Storage Connector | 把不同对象存储协议统一为读/描述接口 | 不保存业务状态 | 模型推理、对象副本决策 |
| Event Ingestor | 将完成事件持久化为 Job 后 ACK | event ledger、Job | 直接做慢模型推理 |
| CPU Pipeline | EXIF、preview、缩略图、质量、pHash | Derivative metadata | GPU 调度、原图真相 |
| GPU Pipeline | image/text embedding、OCR/VLM（后续） | model outputs | Object commit、用户授权 |
| Asset Catalog | Asset、版本、标签、Dataset、血缘 | PostgreSQL | 文件 Body |
| Vector Index | 兼容模型内的 ANN Top-K | pgvector/未来 vector DB | 权限绕过、原图存储 |
| Query/RAG API | 身份范围、检索、证据、答案 | 查询审计 | DataNode 直连 |

二进制派生物（缩略图、preview、OCR 原始结果）不应长期只放在 Worker 本地磁盘。目标是把它们写入某个对象存储的
`derivatives/` 命名空间，而 PostgreSQL 只保存其版本、位置、校验和和状态。

## 5. 统一接入契约

CineLake 用 Adapter 隔离对象存储差异：

```python
class StorageConnector:
    def describe(self, source_ref) -> ObjectDescriptor: ...
    def open_read(self, descriptor, capability) -> BinaryStream: ...
    def list_committed(self, cursor, limit) -> Page[ObjectDescriptor]: ...
```

两个首批实现：

```text
MinidriverHttpConnector
  Redis FILE_UPLOAD_COMMITTED
  Gateway manifest → DataNode Chunk stream → hash verification

S3Connector
  S3/MinIO event notification
  HEAD Object + GET Object(versionId)
  SDK/SigV4 + IAM/temporary credentials
```

无论来源如何，进入 CineLake 后都变成：

```text
source / objectId / objectVersion / objectKey / contentHash / contentType
```

详细协议、回填和版本语义见
[MiniDriver/S3 Storage Connector 设计](CINELAKE_MINIDRIVER_AND_S3_STORAGE_CONNECTOR_DESIGN_2026-08-25.md)。

## 6. 任务与数据流

### 6.1 新上传流

```text
Browser → MiniDriver：上传并 Commit
Gateway Outbox → FILE_UPLOAD_COMMITTED
Ingestor → PostgreSQL IndexJob（先持久化）→ ACK event
CPU Worker → thumbnail / EXIF / preview / quality
GPU Worker → image embedding
Catalog + Vector Index → Asset READY
```

上传成功不等待 AI。用户界面同时显示：

```text
对象：已安全保存
AI：排队中 / 索引中 / 已可搜索 / 失败可重试
```

### 6.2 历史 Backfill 流

```text
Catalog Scanner
  → 分页读取已 COMMITTED 对象
  → 生成幂等 IndexJob
  → 保存 cursor/checkpoint
  → CPU/GPU Worker 按能力缓慢消费
```

新上传任务优先级高于 Backfill；模型升级或历史回填不能挤占用户的新上传体验。

## 7. 搜索与 RAG 流

```text
用户查询
  → Query API 验证 user/tenant/Dataset scope
  → 文本 embedding
  → SQL 元数据过滤（时间、相机、目录、状态）
  → pgvector ANN 取 Top-N
  → 质量/业务 rerank，留下 Top-K
  → 返回对象引用，或交给 LLM 生成带证据的回答
```

RAG 不能把全库向量或原图提交给大模型：

```text
LLM 输入 = 用户问题 + Top-K 的已授权 metadata/标签/摘要/缩略图引用
LLM 输出 = 回答 + evidence(objectId, objectVersion, score)
```

文本检索和图片检索本身不必依赖 LLM；Embedding + Vector DB 已足够。LLM 是“解释、推荐、规划”的上层能力。

## 8. 高并发与隔离原则

```text
上传并发：MiniDriver Gateway/DataNode 负责；AI 不处于同步路径。
任务并发：Redis/Kafka + Durable Job；CPU/GPU pool 各自有界。
查询并发：无状态 Query API 横向扩展；Vector DB 在服务端完成 ANN。
RAG 并发：单独的 LLM request queue；不能抢占 embedding 回填队列。
```

必须配置的资源边界：

```text
browser concurrent files        3～6（客户端策略）
CPU decode workers              有界
GPU embedding batch/concurrency 有界
LLM concurrent generations      有界
pending/retry/DLQ queue          有界且可观测
object read bytes/timeouts       有界
```

## 9. 四机演示部署目标

第一版不要求 Kubernetes。每台机器用 Docker Compose/系统服务运行明确组件即可：

```text
Host A/B/C
  MiniDriver：Gateway / Metadata Member / DataNode（按 V3-Lite 拓扑分布）

GPU Host
  CineLake：PostgreSQL + pgvector、Redis、API、CPU Worker、GPU Worker、optional LLM server
```

真实部署时可将 CPU Worker 扩到非 GPU 主机；GPU Host 只承担有限的模型任务。Gateway、Raft 和 DataNode 不能依赖
GPU Host 可用。

## 10. 演进边界

```text
第一版：MiniDriver HTTP + Redis + PostgreSQL/pgvector + 一个 GPU Worker + 检索/RAG Lite
规模增长：多 CPU/GPU Worker、HNSW、读缓存、指标和限流
明确瓶颈后：Kafka、独立 Vector DB、Spark Backfill、Kubernetes/k3s
```

Spark 适合历史大量对象的批量重算、统计、Dataset 导出；不应用于一次用户搜索或一次 RAG 请求。

## 11. 成功标准

```text
对象存储故障不会由 AI 放大为数据丢失。
同一对象版本的重复事件不会生成重复资产或无限任务。
新上传不等待 GPU，历史 Backfill 不饿死新任务。
搜索只传 Query 与 Top-K 结果，不传输全库向量。
每个 RAG 回答都能回到 objectId + objectVersion 的证据。
```
