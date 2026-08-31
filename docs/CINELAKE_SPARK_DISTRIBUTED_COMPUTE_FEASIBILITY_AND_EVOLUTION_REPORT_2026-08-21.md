# CineLake：Spark 分布式计算可行性、当前瓶颈与服务拆分路线（2026-08-21）

## 一句话结论

**现在不应该为了“看起来分布式”立刻部署 Spark。** 当前系统的主要限制是单机 CPU 上的 OpenCLIP 推理、单个
Asset Worker 和尚未量化的跨主机对象读取，而不是 Gateway 的位置；先把 AI Worker 做成可横向扩展且可观测的
服务，才有证据决定是否引入 Spark。

但 MiniDrive 与 CineLake 应当从现在开始按两个独立服务设计：

```text
MiniDrive = 分布式对象存储与对象事件来源
CineLake  = 数据资产 Catalog、派生处理、向量检索、Dataset 与分析计算
```

它们通过版本化事件与受控对象读取 API 对接，而不是让 Spark、AI 或数据库侵入 Gateway 的 C++ 数据面。逻辑上可
独立部署；当前保留同一仓库有利于联调，待契约稳定后再拆为独立仓库/发布单元。

## 1. 当前系统到底是什么

```text
Browser
  → MiniDrive Gateway / DataNode                         （分布式存储与传输）
  → Gateway LevelDB Outbox → Redis Stream
  → CineLake stream-worker → PostgreSQL Job
  → CineLake Asset Worker → Gateway manifest → DataNode Chunk
  → EXIF / quality / OpenCLIP → PostgreSQL + pgvector
  → AI API → 前端搜索 / Dataset / 只读 Agent
```

当前 AI 处理是**单机的异步多进程流水线**，不是 Spark/Flink 的分布式计算：

- Redis Stream 提供事件解耦和 Consumer Group，不等于分布式计算框架；
- PostgreSQL `FOR UPDATE SKIP LOCKED` 支持多个 Worker 避免重复领取 Job，但当前 Compose 只启动一个
  `worker`；
- pgvector 是在线向量检索后端，不是用于大规模 ETL 的计算引擎；
- MiniDrive 的 Gateway/DataNode 是多节点存储系统，但 AI 推理目前集中在 `ubuntu22data1`。

## 2. 当前实测证据与瓶颈判断

### 2.1 已测到的限制

真实 OpenCLIP 基线使用 56 张 JPEG（约 572 MiB）、CPU、`ViT-B-32-quickgelu/openai`、安全的 batch=1：

| 指标 | 结果 | 说明 |
|---|---:|---|
| 成功索引 | 56 / 56 | 正确性链路可完成 |
| 总索引耗时 | 161.72 s | 包含模型加载、JPEG 读取与全部处理 |
| 平均耗时 | 2.89 s / 图 | 当前最直观的容量信号 |
| 峰值 RSS | 约 1.43 GiB | batch/GPU 扩大前必须考虑内存预算 |
| Top-5 Query Hit Rate | 1.000 | 小型固定集的质量信号，不是吞吐指标 |

来源：[Phase C 真实 OpenCLIP CPU 评测](CINELAKE_PHASE_C_SEMANTIC_INDEX_ARCHITECTURE_AND_TEST_REPORT_2026-08-21.md)。

因此当前优先级为：

```text
1. OpenCLIP CPU 推理与 JPEG 解码              已有直接证据
2. 单 Worker 并行度 / batch / 内存预算         已有直接证据
3. Worker 跨 Tailnet 的对象 materialize 时间   尚未单独量化
4. PostgreSQL exact vector scan                56 张规模下没有瓶颈证据
5. Gateway 路由/manifest QPS                   尚未单独量化
```

不能把“Gateway 在另一台机器”直接等同于“必须用 Spark”。Gateway 在当前对象读取路径中只负责 manifest/
路由控制，图片 Body 由 Worker 直接从 DataNode 副本读取；它不应成为模型计算的执行节点。

### 2.2 尚未测量、但必须补齐的分解指标

一次索引的端到端时间应拆为：

```text
event_lag
  + PostgreSQL job_queue_wait
  + Gateway manifest_latency
  + DataNode body_download_time / bytes
  + JPEG/RAW decode_time
  + EXIF/quality_time
  + embedding_inference_time
  + PostgreSQL upsert_time
```

当前 161.72 秒是总时间，不能用它判断网络、Gateway 或模型各自占多少。先加入这组 stage metrics，并在
`worker=1/2/4`、`batch=1/2/4`、不同对象大小、不同主机位置下做矩阵，才有引入 Spark 的工程依据。

### 2.3 当前可靠性瓶颈

PostgreSQL 的 `SKIP LOCKED` 能避免多个在线 Worker 重复认领 pending Job，但“Worker 在 processing 状态崩溃后
如何 lease 超时回收”仍未完成。没有 lease、重试退避、DLQ 和 stage 级幂等约束时，先扩大 Worker/Spark 只会扩大
失败恢复范围。

第一优先级不是 Spark，而是：

```text
Job lease / heartbeat / reclaim
指数退避与 DLQ
(asset_id, object_version, stage, processor_version) 幂等键
每阶段耗时、失败原因、重试次数与 backlog 指标
```

## 3. Gateway 在异机时，Spark 是否仍合适

**合适，但 Gateway 不应承担 Spark 数据中转。**

当前三机布局：

```text
Gateway      100.75.93.124  控制面、manifest、网页
DataNode     100.75.72.15   部分对象副本
CineLake     100.89.50.125  Redis、PostgreSQL、当前 AI Worker
```

将来 Spark Executor 可以运行在独立计算机或 GPU 节点，通过同一受控接口：

```text
Spark executor
  → Gateway: GET manifest(object_id, version)
  → Replica DataNode: GET chunk body
  → 本地临时文件 / decode / inference
  → 写入派生结果与 ProcessingRun
```

Gateway 的异机位置只增加 manifest 控制请求 RTT；真正可能昂贵的是 Executor 到 DataNode 的对象 Body 网络传输。
这仍不是 Spark 的阻碍，而是数据局部性问题：

- 第一版可接受远程读取，靠有界并发、对象大小上限和 DataNode 下载配额保护存储；
- 后续可在 event/manifest 中携带 replica 地址，让 Executor 优先读网络更近或负载更低的副本；
- 不应把所有图片先经过 Gateway、Redis 或 PostgreSQL 再交给 Executor；
- DataNode 是存储节点，不应为了凑 Spark 集群而直接塞入重 CPU/GPU Executor，以免挤压副本写入、磁盘和网络。

## 4. Spark 应放在什么位置

Spark 不替代在线索引 Worker。正确分工如下：

| 场景 | 推荐组件 | 原因 |
|---|---|---|
| 单个上传后的秒/分钟级索引 | Redis + 常驻 Asset Worker | 延迟低、流程简单、失败独立 |
| 少量 Worker 横向扩展 | PostgreSQL Job + 多个 Worker | 先验证计算/网络上限，成本最低 |
| 更换 CLIP 模型后历史全量重算 | Spark Batch | 大量独立对象、可分区、可重试 |
| 大规模质量统计、重复候选、Dataset 报告 | Spark SQL | 结构化 metadata 批量扫描/聚合 |
| 每日/每小时 event 聚合 | Spark Structured Streaming 或 Flink | 有 checkpoint、窗口和状态管理需求时 |
| 在线 Top-K 检索 | pgvector/Milvus + Query API | Spark 不适合交互式低延迟查询 |

### 4.1 目标批处理架构

```text
MiniDrive ObjectCommit
  → Redis Streams（在线增量）
  → CineLake PostgreSQL / ProcessingRun
  → 周期同步或 Kafka 入湖
  → Iceberg tables（asset_version / processing_run / quality / dataset_member）
  → Spark Batch
       ├─ 指定 Iceberg snapshot 读取待回填 AssetVersion
       ├─ 按 object_id/version 分区
       ├─ Executor 通过 MiniDrive Reader 读取对象
       ├─ GPU/CPU inference service 或分区批量 UDF
       ├─ 写入 embedding/quality 结果表、失败表
       └─ 发布可索引的派生结果
  → 在线索引投影到 PostgreSQL + pgvector（小规模）或专用向量库（大规模）
```

关键约束：消息只传对象身份；Spark 读取对象 Body 时直接访问 Storage API；原对象永远不写入 Redis/Kafka/Iceberg
元数据表。

### 4.2 Iceberg 的正确使用方式

Iceberg 保存的是分析型 Parquet 表，不是 MiniDrive 的替代品：

```text
asset_version(object_id, object_version, object_uri, sha256, size, media_type, committed_at)
processing_run(object_id, object_version, stage, processor_version, status, timings, error)
embedding_manifest(object_id, object_version, model_id, vector_uri/version, generated_at)
dataset_member(dataset_id, asset_id, object_version, role, snapshot_at)
```

当前 MiniDrive 不是 S3 兼容 Iceberg FileIO，Spark/Iceberg 不能不加适配器就把 Parquet warehouse 写到
`minidrive://`。因此分两步做：

1. 学习/原型期：Iceberg warehouse 放在本地文件系统、MinIO/S3 或已有 HDFS；表中存 MiniDrive 的逻辑
   `object_uri`，Spark 通过 Reader 读取 RAW/JPEG Body；
2. 存储 API 稳定后：若确有必要，再实现 MiniDrive Iceberg FileIO/S3 兼容层。不能在没有事务、列式文件
   原子提交和 Catalog 语义时宣称“MiniDrive 已是湖仓”。

## 5. 最小 Spark 设计，而非一次性上大集群

### Phase S0：先测现有 Worker（现在必须做）

不新增 Spark。补充 stage metrics，记录同一固定素材集上的：

```text
worker 数、batch、CPU/GPU、RSS、event lag、queue wait
manifest/download/decode/inference/upsert P50/P95
成功率、失败重试、每图/每 GiB 成本
```

验收：能明确回答“推理、网络、解码、数据库哪个占主导”。

### Phase S1：多 Worker / 多机 Worker

将同一个容器镜像部署到第二台**计算节点**，两个 Worker 共用 PostgreSQL 与 Redis Consumer Group；每个 Worker 使用
唯一 consumer name。加入 Job lease/reclaim 后再提高并发。此阶段已经是最小的分布式任务处理，且比 Spark 更贴近
当前逐对象索引工作负载。

验收：Worker 失败可回收；吞吐近似随 Worker 增加而提高；DataNode 下载、Gateway manifest 和 PostgreSQL P95
不出现不可接受退化。

### Phase S2：单机 Spark Local Mode

仅用于学习 DataFrame、Parquet、Iceberg snapshot、分区和 checkpoint：从 PostgreSQL 导出/构造少量
`asset_version` 表，在 125 或开发机跑 `local[*]`。它不是分布式计算成果，也不应写成集群性能。

验收：能用一个 Iceberg snapshot 可重复生成质量统计/待重建对象列表，并用幂等键写 ProcessingRun。

### Phase S3：Spark 批量回填集群

只有出现明确需求时再做：例如 CLIP 模型升级，需要重建数万历史对象；或 Dataset/质量表已大到 PostgreSQL
交互查询不适合全表扫描。届时部署独立 Spark driver/executor 与独立 warehouse，不与 Gateway/DataNode 竞争资源。

验收：固定 snapshot 的重算可恢复、重复提交不重复写、失败分区可重跑；同时记录每 executor 的对象读取量、
推理耗时、shuffle、失败率、总成本和端到端吞吐。

## 6. 服务拆分建议

### 6.1 现在：代码可以同仓库，进程必须独立

```text
miniDriver/                 C++ 存储、Gateway/DataNode、事件 Outbox
ai-app-lite/                Python AI 服务、Compose、PostgreSQL schema、Worker
docs/contracts/             FILE_UPLOAD_COMMITTED / manifest / object-read 契约
```

当前这种目录边界已经合理：AI Compose 不编译或启动 C++ 服务，MiniDrive 也不导入 Python 依赖。

### 6.2 后续：契约稳定后再拆仓库

可拆为：

```text
miniDriver                 独立对象存储项目
cinelake-data-platform    独立 AI Data Infra 项目
```

两个项目只共享版本化契约：

```text
FILE_UPLOAD_COMMITTED vN
GET /api/v2/objects/{id}/manifest
GET /v2/chunks/{hash}
object_id / object_version / SHA-256 / route token / error contract
```

不要现在立刻拆 Git 仓库：事件、对象版本、Worker 身份认证和重试语义仍在演进；过早拆分会让每次联调成本上升。
先将这些接口写入 contract test 和示例事件，完成一轮跨机完整验证后再拆，才会是真正的服务边界。

## 7. 最终判断

```text
Gateway 异机           ≠ 不适合 Spark
Redis Stream            ≠ 分布式计算
多个独立 Worker         = 当前最适合的横向扩展路径
Spark + Iceberg          = 后续历史回填、批量分析与数据版本快照能力
MiniDrive 与 CineLake    = 应独立部署、通过契约耦合，而非代码混在同一进程
```

对当前项目最有价值的下一步不是部署 Spark，而是完成三主机端到端运行后，补 `processing` Job lease/reclaim 与
stage metrics；有了这些数据，才能决定是加一个 GPU Worker、增加普通 Worker，还是在模型升级时引入 Spark。

## 8. 证据索引

- [Phase C 语义索引架构与真实 CPU 基线](CINELAKE_PHASE_C_SEMANTIC_INDEX_ARCHITECTURE_AND_TEST_REPORT_2026-08-21.md)
- [PostgreSQL + pgvector 检索后端](CINELAKE_PHASE_D_POSTGRES_PGVECTOR_IMPLEMENTATION_2026-08-21.md)
- [E0/F0 Dataset 与只读 Agent](CINELAKE_PHASE_E_F_MINIMUM_GOVERNED_AGENT_IMPLEMENTATION_2026-08-21.md)
- [三主机 AI 部署配置](CINELAKE_THREE_HOST_AI_DEPLOYMENT_2026-08-21.md)
- [初始 AI Data Infra 设计与学习指南](CINELAKE_AI_DATA_INFRA_DESIGN_AND_LEARNING_GUIDE_2026-08-14.md)
