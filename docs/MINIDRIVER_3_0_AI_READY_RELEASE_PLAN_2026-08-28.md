# MiniDriver 3.0：AI-Ready 对象存储冻结设计与实施计划（2026-08-28）

> 状态：MiniDriver 3.0 后续工作的主执行文档。
>
> 目标：不再为了单项极限吞吐长期打磨 MiniDriver，而是把它冻结为一个语义正确、持久化明确、接口稳定、可在三台
> 真实主机运行，并能承载 LensCompute 图片读取与派生对象写入的对象存储底座。
>
> 技术依据：完整磁盘、校验、背压和性能实验继续参考
> [高并发与高性能数据面实施方案](MINIDRIVER_HIGH_CONCURRENCY_AND_PERFORMANCE_IMPLEMENTATION_PLAN_2026-08-25.md)；
> 本文只定义 3.0 的发布阻塞项。

### 3.0 范围澄清（2026-08-29）

3.0 的读侧重点已经从“能否下载一个对象”补齐为：**多客户端并发读取时是否仍然高吞吐、低尾延迟、公平且资源有界**。
因此 P6-READ 的 Hot Object、Hot Chunk、随机对象、大小混合、慢读者隔离和 Keep-Alive 都属于 3.0 的性能验收。

下列内容虽然与未来 AI 计算很重要，但依赖健康副本视图、节点负载和独立计算调度器，明确不属于 3.0：

```text
Replica-aware read scheduling（S1 忙、S2 空闲时自动改读 S2）
DataLocalityHint / Object placement affinity
按数据位置和 CPU/GPU 负载决定哪个 LensCompute Worker 执行任务
```

它们在 LensCompute + MiniDriver 4.0 的接口上实现；不能把“尚未实现的调度功能”伪装成一次 benchmark 可以验证的内容。

## 1. 版本结论

MiniDriver 3.0 的成功标准从“完成所有可能的存储优化”调整为：

```text
关键语义 100% 正确
AI 读取接口稳定
性能可解释、资源有界
Docker 可复跑
真实三主机 RF=2 验收通过
```

3.0 不接入 Raft，不实现自动 Repair，不实现多 Gateway。版本边界固定为：

```text
MiniDriver 3.0
  1 Gateway
  1～3 DataNode
  immutable object + stable version
  RF=2 + durable ACK
  HTTP Adapter / Chunk Coordinator / Chunk Store 解耦
  Official Client SDK Core（C++ first；Web/Go/Python thin adapters）
  CRC32C fast-path + opt-in strong-content verification
  Worker Read Capability
  Keep-Alive + verified Chunk Range Read
  AI mixed-object workload
  Docker Compose + 真实三主机验收

MiniDriver 4.0
  薄 Gateway
  3 Metadata/Raft
  nodeEpoch / generation fencing
  Health / Placement / Repair / Read Failover
  3 Gateway + Load Balancer
```

LensCompute 是独立项目。MiniDriver 只提供对象版本、读取授权、直接数据路径和完成事件，不负责决定哪台 CPU/GPU
执行缩略图、鸟检测、向量或降噪任务。

SDK 也不是 LensCompute Scheduler：SDK 负责“数据如何可靠读写”，Go Scheduler 负责“哪个 Worker 执行哪一个
Job”。二者通过对象版本和只读 locality hint 协作，但绝不互相拥有对方的队列或状态。

## 2. 当前已经具备的基础

以下能力已经完成并继续作为 3.0 基线：

```text
客户端直传 DataNode，Gateway 不转发 Chunk Body
4 MiB storage Chunk 与 RF=2 链式副本
SharedBodyBlock / BlockPool / pause-resume 背压
DiskWriteExecutor 与有界准入
256 KiB pwritev 聚合写
buffered / chunk_sync / group_commit 三种持久化模式
8 MiB / 8 items / 2 ms 静态 Group Commit 基线
Primary 与 Replica durable ACK
HTTP Adapter → ChunkWriteCoordinator → ChunkStore / ReplicaTransport
cas-sha256 与 opaque-chunk-id 双格式
SHA-256 / CRC32C Provider 与性能 A/B
Session 预分配 objectId/objectVersion/metadataVersion
Gateway 重启后相同 Session 重试复用 chunkId
Manifest 输出 object/version/chunk/checksum/generation
```

当前缺口不是重新设计数据面，而是冻结对象语义、补 Worker 读取契约、完善批量读取需要的连接/Range、建立真实 AI
负载和可复跑部署。尤其需要将当前仍由浏览器/benchmark 手写的 route、重试、Chunk 拼接和校验逻辑收敛为 SDK。

## 3. 三条必须保持独立的路径

```text
写入路径
  Client/Worker → Gateway Session/Route → DataNode Primary → Replica → Commit

读取路径
  Client/Worker → Gateway Read Plan/Capability → DataNode direct GET/Range

计算路径
  MiniDriver Commit Event → LensCompute Job → CPU/GPU Worker
  → MiniDriver 写入 derivative object
```

禁止形成：

```text
Gateway 转发原图/缩略图 Body
Raft/Redis/PostgreSQL 保存文件 Body
DataNode 进程直接运行 JPEG/YOLO/Embedding
LensCompute 直接打开 DataNode extent 文件
```

## 4. Gate 1：不可变对象与稳定版本

### 4.1 3.0 语义

3.0 不实现原地覆盖。规则固定为：

```text
Session 创建时分配 objectId
objectVersion = 1
COMMITTED 后 Body 永久不可修改
同一路径再次创建返回 409 PATH_CONFLICT
内容变化必须创建新的 objectId/路径
metadataVersion 只为后续元数据变化预留
所有 AI Job 使用 objectId + objectVersion
```

`fileHash/contentHash` 是内容属性，不是逻辑对象身份。相同内容可以对应不同逻辑对象；物理去重是否发生不得改变
`objectId + objectVersion`。

### 4.2 必做任务

- [x] Session 创建时持久化 `objectId/objectVersion/metadataVersion`；
- [x] Route Capability、Manifest、File/Object Commit 贯穿相同身份；
- [x] Gateway 重启后同一 Session 重试生成相同 opaque `chunkId`；
- [x] 增加不可变对象测试：COMMITTED 对象不能再次写入或更换 Manifest；
- [x] 增加路径冲突、重复 Commit 与删除后读取测试；
- [x] 增加旧格式 Session 恢复与重放测试（当前覆盖最早10字段格式，解析器继续兼容11/13字段）；
- [x] API 响应统一返回 `objectId/objectVersion/metadataVersion`，旧字段保持兼容；
- [x] 文档明确 4.0 才增加 `CreateObjectVersion(expectedCurrentVersion)`。

### 4.3 验收

```text
相同业务重试不产生第二个 chunkId 或 objectId
同一路径覆盖稳定返回 409
AI Event、Manifest、Commit 返回同一 objectId/version
旧格式 Session/Object/Route 仍能读取
```

## 4.5 Gate 1.5：Client SDK 与校验策略收口

### 4.5.1 SDK 的边界

SDK 是 MiniDriver 对外的数据访问面，不是新的 Metadata Server，也不是分布式计算调度器：

```text
LensCompute Go Scheduler
  输入：Job priority / Worker CPU-GPU capability / load / optional locality hint
  输出：把 Job 投递给 S1/S2/S3 的一个 Worker

MiniDriver Client SDK（运行在该 Worker 内）
  输入：objectId + objectVersion + Read/Write options
  输出：经过授权、校验、重试后的 Object stream
```

3.0 交付 `minidriver-client-core` 的 C++ 实现和协议契约；benchmark/CLI 是首批消费者。Web、Go、Python 先提供
同一 HTTP 合约的薄 Adapter/参考实现，不要求在 3.0 发布前完成三套功能不同的完整 SDK。

最小 API：

```cpp
UploadSession beginUpload(UploadRequest);
ObjectRef UploadSession::writeFrom(Source) / commit();
ObjectStream openObject(ObjectRef, ReadOptions);
RangeStream openRange(ObjectRef, offset, length, ReadOptions);
ReadPlan getReadPlan(ObjectRef);
```

SDK 负责 Session/Capability、Chunk 切分、有界 window、连接复用、按 ReadPlan 的有限副本重试、流式校验和指标；
它不直接访问 extent，不宣布节点 OFFLINE，不修改副本状态，也不选择计算 Worker。

### 4.5.2 新的身份与校验模型

```text
逻辑身份： objectId + objectVersion
物理身份： chunkId（不透明、由 Gateway/Session 稳定分配）
传输校验： checksumType + checksumDigest
强内容属性： optional contentHash（不能替代 object/version）
```

三个大小严格分离：

```text
storageChunkBytes       4 MiB：路由、副本、Commit 的单位
checksumSegmentBytes  256 KiB：Range 局部完整性校验的单位
targetBatchBytes      256 KiB：DataNode 一次 pwritev 聚合的目标
```

它们可以在第一版取相同/相近数值，但协议和代码不得假设三者相等。

### 4.5.3 两种明确模式

| 模式 | SDK | Primary / Replica | 去重语义 |
|---|---|---|---|
| `fast-crc32c`（3.0 默认） | 为每个已缓冲 Chunk 算 CRC32C；V2 兼容字段 `hash` 仅携带稳定 `sessionId + chunkIndex` 路由键后再上传 | 两个 DataNode 都流式验证 CRC32C；按 durability policy ACK | 不做“客户端 hash 声明即命中”的同步精确去重 |
| `strong-content`（opt-in） | 提供 contentHash | Primary 必须重算强摘要并验证；Replica 至少验证传输 checksum | 只有可信强摘要验证后才允许建立 contentHash → physical storage 的去重引用 |

因此现有 `cas-sha256` 继续作为 legacy compatibility scheme；`opaque-chunk-id + crc32c` 是 3.0 新对象的默认协议。
CRC32C 用于发现随机传输/存储损坏，不是抗碰撞内容身份证明；不能因切到 CRC32C 就继续信任未验证的客户端 SHA 进行精确去重。
SDK 对 `fast-crc32c` 不再预先计算 SHA-256；Gateway 生成不透明 `chunkId`，而兼容路由键只要求同一 Session 重试时稳定。
需要创建 legacy CAS 对象的部署可显式设置 `MINIKV_V3_IDENTITY_SCHEME=cas-sha256` 与
`MINIKV_V3_CHECKSUM_TYPE=sha256`；已提交旧对象始终按其记录的 identity/checksum 读取。

默认值已做隔离验证：不设置任意 `MINIKV_V3_*` 协议环境变量时，1 Gateway + 2 DataNode、RF=2 的 SDK V3
ReadPlan 链路完成 `4/4` 个 4 MiB 对象；原始结果为
`/tmp/minidriver-v3-sdk-default-e2e-results-20260830/`。这证明默认协议实际可用，不替代 legacy CAS 的兼容回归。

SDK/benchmark 以 `--upload-checksum crc32c|sha256` 显式匹配 Gateway Session 的协商策略；不匹配时 SDK 拒绝上传，
不会把 CRC 数据错误标记为 SHA 数据。`opaque+crc32c` 默认与显式 `cas+sha256` 的 RF=2 协议烟测均覆盖
64 KiB、4 MiB、16 MiB 端到端上传、下载与最终 SHA 校验，原始结果分别为
`/tmp/minidriver-v3-p5-default-protocol-20260830/` 和
`/tmp/minidriver-v3-p5-legacy-protocol-20260830b/`。

V3 专用 P5/P6/R5 脚本现默认要求 `MINIKV_V2_CLUSTER_SECRET` 并附带 `--sdk-read-plan true`、集群令牌和
service principal；只有设置相应 `MINIKV_V3_*_SDK_READ_PLAN=false` 才会回退 legacy Adapter。更新后的 P5 默认烟测已
在 `/tmp/minidriver-v3-p5-sdk-readplan-default-20260830/` 通过，因此该默认不是未执行的脚本文字。

### 4.5.4 上传和下载的正式流程

```text
Upload
SDK 按 4 MiB 读取到有界 buffer → 计算本 Chunk checksum
  → Gateway Begin/Route 返回 chunkId + Capability + replica chain
  → Primary 流式 checksum + 聚合写 → Replica 流式 checksum + 写盘
  → 本地/副本/durability 条件满足 → Gateway CommitChunk
  → 全部 Chunk 已提交 → CommitObject/Manifest

Download
SDK getReadPlan(objectId, version)
  → 从计划的首选/本地候选读 GET 或 Range
  → 按 whole/segment checksum 流式校验
  → 同一计划内有限 fallback 到另一个候选副本
  → 向 JPEG decoder、文件或调用者提供顺序 Object stream
```

完整对象读可以验证 whole checksum；对象/Chunk Range 只有在覆盖的每个 segment 都有 sidecar digest 时才可称为
“verified range”。无 sidecar 的 Range 只作为兼容读取能力，SDK 必须显式标记 `integrity=unverified-partial`，不能伪称
完整 Chunk 已校验。

### 4.5.5 必做任务与验收

截至 2026-08-30，本 Gate 已开始实现，但尚未完成发布验收。当前 C++ Core 位于
`include/client/`、`src/client/`，并被 `minikv_benchmark` 作为唯一上传/下载实现使用；原 benchmark 的 HTTP
transport 已降为兼容别名，避免两套连接复用和校验逻辑继续分叉。

- [~] 写 `Client SDK + Integrity Contract`：ReadPlan/legacy manifest 兼容、whole-chunk CRC32C/SHA-256 验证、
  有界副本 fallback、Keep-Alive 统计和错误文本已在 C++ Core 中落地；规范化错误码、Capability 过期/撤销语义和
  完整兼容矩阵仍待补齐；
- [x] 抽取 C++ `minidriver-client-core`，使 benchmark 上传、下载 profile 经 SDK 而非手写 HTTP 流程；
- [~] SDK Upload 固定使用原 sessionId/chunkId 重试，且有 global bytes/chunk window 上界；当前 SDK 用同一
  `sessionId + chunkIndex` 进行最多 6 次 Route admission 重试，`chunkWindow` 限制实例内并发，benchmark 通过
  admission hook 保留 global chunk budget。V3 opaque `chunkId` 默认上传、断线后 body 重传和跨进程恢复测试仍待完成；
- [~] SDK Read 只使用 Gateway 签发的 ReadPlan；副本 fallback 有次数、超时和可观测结果；C++ Core 已支持 V3
  ReadPlan + Capability，benchmark 已可显式以 Commit 返回的 `objectId + objectVersion` 走完整 V3 读取链路。
  `test_minidriver_client` 覆盖 Capability 转发、坏副本 checksum 拒绝、第二副本 fallback 与对象级 Range；
  `minidriver_client_worker` 是首个只经 SDK 读取 ObjectRef 的原生 Worker demo。旧 benchmark/Web 默认仍保留 legacy
  manifest Adapter，直到完整兼容矩阵完成后才切换默认；
- [ ] Gateway/DataNode 支持 `checksumSegmentBytes + sidecar` 的写入、Manifest/read-plan 传递和读取验证；
- [ ] 明确并测试 fast-crc32c 不提前命中精确去重；strong-content 不验证强摘要不得建立去重引用；
- [ ] 旧 V2 Web API / `cas-sha256` 对象继续可读写；新 SDK API 通过 Adapter 与旧协议共存。

当前 SDK core 的明确限制：它每次只将一个 storage Chunk（当前通常 4 MiB）保存在内存中再交给调用方，因此内存有界；
它已提供对象级 `downloadRangeToFile()`，将对象 offset/length 映射为一个或多个 Chunk GET/Range，但尚未提供对象级
streaming decoder 回调、verified Range sidecar 或动态副本选择。这些能力不能因
benchmark 已迁移而提前宣称完成。

SDK benchmark 已完成两条 RF=2 集成语义验证：2 个并发 16 MiB 对象、每对象 4 Chunk，均通过 Upload → Commit →
Download → 最终 whole-file SHA-256；其中第二条显式使用 Commit 返回的 `objectId + objectVersion` 请求 V3 ReadPlan 和
短期 Read Capability，而不是从 legacy manifest 反推对象身份。原始 CSV 分别位于
`/tmp/minidriver-v3-sdk-e2e-20260829d/` 与 `/tmp/minidriver-v3-sdk-readplan-results-20260829f/`。该过程发现并修复
legacy manifest 缺少逐 Chunk `size` 的适配，以及 Route admission 503 的有界重试；它不是 P6/R5/R7 性能验收的替代。

第三条集成验证固定 Gateway 为 `opaque-chunk-id + crc32c`：SDK 上传时只计算 CRC32C，并以稳定的
`upload:{sessionId}:{chunkIndex}` 作为兼容 Route key；4/4 个 16 MiB 对象仍完成 RF=2 Upload → V3 ReadPlan →
整 Chunk CRC32C → final whole-file SHA-256。原始结果位于
`/tmp/minidriver-v3-sdk-crc-e2e-results-20260830/`；两轮 aggregate 为 `68.10`、`65.85 MiB/s`，这只是语义验证，
不是新的性能峰值。

代码回归方面，2026-08-30 在允许本地 socket/临时 Redis 进程的执行环境运行 `ctest --test-dir build --output-on-failure`，
SDK、Gateway、DataNode、benchmark 与 metadata 相关测试均通过；总计 `58/59` 通过。唯一失败是既有
`v2_thumbnail_contact_sheet_ui` 缺少 Python `playwright` 模块，和 SDK/协议代码无关，仍应在发布环境安装 UI 测试依赖后
复跑，不可直接写成全绿。

**验收：** 同一个 Upload/Download API 在 benchmark、CLI 和一个 Worker demo 中语义一致；benchmark/Worker 默认
入口使用 Commit 返回的 ObjectRef；网络重试不制造第二个逻辑 Chunk；Range 结果的 `verified/unverified` 状态可以被
调用方观察；SDK 不泄露 DataNode extent 路径。

首个 Worker demo 已交付为 `build/bin/minidriver_client_worker`：

```text
minidriver_client_worker read
  --gateway HOST:PORT --cluster-token TOKEN --service-principal WORKER_ID
  --object-id ID --object-version N --output /absolute/output
  [--range OFFSET:LENGTH]
```

它没有 DataNode 磁盘目录参数；只接受 Gateway 端点和固定 ObjectRef。2026-08-30 的临时 RF=2 集成使用 benchmark
上传对象后，从 `runs.csv` 的 `object_id/object_version` 传给该 Worker；Worker 经 V3 ReadPlan 下载的 SHA-256 与输入相同。
证据保留在 `/tmp/minidriver-v3-sdk-worker-demo-20260830b/`。这是 Worker 数据访问面已打通的证据，不替代三主机、撤销
Capability 或 Range sidecar 验收。

### 4.5.6 Gateway 迁移边界：收口而非重写

Gateway 不重写 HTTP server、EventLoop 或 DataNode 直传路径；它要做的是把旧 V2 Handler 收敛为 SDK 协议的
兼容 Adapter：

```text
保留：HTTP入口、Session/Lease、Capability签发、Manifest、Commit、LevelDB当前实现
修改：Route/Commit 的主键和记录字段 → object/version/chunkId/checksum
新增：SDK Begin/Route/ReadPlan 统一契约、segment sidecar 引用、校验策略
兼容：/api/v2/... 继续翻译为同一内部 Object/Chunk command
禁止：Gateway 代理 Body、以客户端声称的 SHA 直接建立可信去重引用
```

因此工作量主要在 GatewayState/协议对象/持久化兼容迁移，而不是网络框架重写。迁移期间 Manifest 必须明确
`identityScheme`；legacy `cas:{chunkHash}` 与 target `chunk:{chunkId}` 双读，旧对象不原地改写。

## 5. Gate 2：LensCompute Worker 受控读取契约

### 5.1 目标接口

LensCompute 不依赖 DataNode 本地目录，只依赖一个稳定的对象读取计划：

```text
POST /internal/v3/objects/{objectId}/versions/{objectVersion}/read-plan
```

目标响应：

```json
{
  "objectId": "obj-123",
  "objectVersion": 1,
  "fileSize": 52428800,
  "chunkSize": 4194304,
  "chunks": [
    {
      "index": 0,
      "chunkId": "chk-001",
      "storageIdentity": "chk-001",
      "size": 4194304,
      "checksumType": "crc32c",
      "checksumDigest": "1234abcd",
      "checksumSegmentBytes": 262144,
      "checksumSidecar": "opaque-sidecar-reference",
      "readCapability": "opaque-signed-token-for-this-chunk",
      "replicas": [
        {"nodeId": "dn-2", "address": "10.0.0.2", "port": 19202}
      ]
    }
  ]
}
```

DataNode 读取：

```text
GET /internal/v3/chunks/{storageIdentity}
X-Read-Token: ...
Range: bytes=start-end            # 可选
```

### 5.2 Read Capability 最小字段

```text
schemaVersion
principalId = service:lenscompute
objectId
objectVersion
storageIdentity（3.0第一版每个Chunk签发一个Token，避免大Manifest Token）
scope = object:read
expiresAt
nonce/capabilityId
HMAC signature
```

第一版只用于可信内网服务身份，不引入 MySQL 用户系统。浏览器权限仍由 LensGrid Backend 决定；MiniDriver只验证短期
能力令牌。

### 5.3 必做任务

- [x] 定义协议无关 `ObjectReadDescriptor/ChunkReadDescriptor`；
- [x] 定义并测试 Read Capability 编解码、过期、篡改和错误密钥拒绝；
- [x] Gateway 根据固定 object/version 生成 read plan；
- [x] DataNode 内部读取入口验证 Capability 后直接发送 Body；
- [x] SDK读取失败时可在同一 ReadPlan 的静态候选副本中有限重试；`test_minidriver_client` 验证首副本 checksum
  不匹配时拒绝数据并切到第二候选。4.0 再由 Metadata 提供动态健康/负载 Read Failover；
- [~] 保留现有Manifest/Chunk GET兼容路径，但标记为 legacy compatibility/testing Adapter；Worker 应使用 V3
  ReadPlan，Web API 的迁移提示和完整兼容矩阵仍待补齐。

### 5.4 验收

```text
Worker不知道extent路径
错误object/version的Read Plan被Gateway拒绝；过期、篡改、错误token被DataNode拒绝
图片Body不经过Gateway
同一read plan可以经SDK读取并按声明的checksum校验完整对象
```

### 5.5 Round 1实现检查点（2026-08-28）

当前已落地：

```text
GatewayState::buildObjectReadDescriptor()
POST /internal/v3/objects/{objectId}/versions/{objectVersion}/read-plan
GET/HEAD /internal/v3/chunks/{storageIdentity}
X-Read-Token（HMAC-SHA256、短期、绑定principal/object/version/storageIdentity）
```

Read Capability按Chunk签发。它授权固定`storageIdentity`，因此DataNode无需读取Gateway LevelDB或理解Manifest；
`objectId/objectVersion`保留在Token和响应Header中用于审计。当前已完成编译和单元测试，真正跨进程读取、健康副本重试与
整对象SHA拼接验证留在Round 1集成测试/真实部署验收中。

## 6. Gate 3：Keep-Alive 与 Chunk Range Read

### 6.1 当前缺口

HTTP框架具备Keep-Alive，但此前基准客户端总是`Connection: close`，而DataNode下载也没有稳定的顺序复用验证。
此前也没有解析HTTP `Range`，只能返回完整Chunk；基础实现现已完成，仍需真实集群集成验证。

### 6.2 Keep-Alive任务

- [x] Benchmark客户端支持顺序Keep-Alive，并保留`close`对照；同一目标DataNode可复用连接，目标切换或失败时强制重连；
- [x] DataNode下载不再无条件关闭HTTP连接；
- [ ] DataNode在sendfile完成后安全释放DownloadLease并复用连接的跨进程集成测试；
- [ ] 连续GET、HEAD→GET、404/416→GET、客户端中断后重新请求均正确；
- [ ] 设置每连接最大请求数、空闲超时和output watermark；
- [ ] 输出连接建立、复用、关闭原因和每连接请求数指标。
- [x] 单元测试验证同一连接上两个分片HTTP响应连续读取正确；

### 6.3 Range语义

3.0只支持单Range：

```text
Range: bytes=start-end
Range: bytes=start-
```

返回：

```text
206 Partial Content
Accept-Ranges: bytes
Content-Range: bytes start-end/total
Content-Length: selected length
```

非法或越界请求返回`416 Range Not Satisfiable`。暂不支持multipart ranges。

Range只作用于单个Chunk。LensCompute StorageConnector/SDK负责将对象级`offset + length`映射为一个或多个Chunk Range，
避免Gateway代理对象Body。完整 Chunk 可以使用 whole checksum 校验；部分 Range 仅当计划提供覆盖区间的
`checksumSegmentBytes + checksumSidecar` 时才返回 `verified`。

### 6.4 Range任务

- [x] 增加严格Range解析器和独立单元测试；
- [x] `FastDataStore::getRegion()`结果叠加Range得到新的`offset/length`；
- [x] GET/HEAD返回一致的206/416/Header；
- [~] SDK `downloadRangeToFile()` 已把对象级 offset/length 映射为 Chunk Range，并在 `test_minidriver_client`
  验证 `Range: bytes=2-5`、206、长度和 `unverified-partial` 结果；首字节、尾字节、完整Chunk、空Range、越界和
  跨Chunk的真实 Gateway/DataNode 集成矩阵仍待完成；
- [ ] Range读取仍受Download admission、Capability和sendfile量子控制。
- [ ] 写入并读取每 256 KiB segment checksum sidecar；SDK 对完整覆盖的 segment 验证后显式返回 `verified`；
- [ ] 没有 sidecar 的 legacy object Range 返回 `unverified-partial`，调用方不得把它显示成完整性已验证。

## 7. Gate 4 / P6-READ：并发读取与AI对象负载基准

这一Gate只测量**现有读数据面**的能力：很多客户端同时拿数据时，吞吐、尾延迟、公平性和资源边界是否成立。
它不实现副本感知读调度或数据亲和性；那是 LensCompute 阶段的功能，不是 benchmark 项目。

### 7.1 固定对象集与测试口径

```text
100 MiB original（Hot Object）
4 MiB storage Chunk（Hot Chunk）
64 KiB thumbnail / 150 KiB thumbnail / 1 MiB preview / 16 MiB JPEG
50 MiB RAW-like original（large + small mixed）
```

第一版可使用确定性随机字节验证存储路径，接入 LensCompute 后补真实 NEF/CR3/JPEG/WebP 集合。每组必须明确标注：

```text
Cold：在可控环境中失效/绕过页缓存后开始；若主机权限不足，报告必须标记“非严格冷读”。
Warm：先用同一对象完成预热读取，再开始正式计时。
```

### 7.2 P6-READ 矩阵

| 编号 | 负载 | 并发 | 必须回答的问题 |
|---|---|---|---|
| R1 Hot Object | 同一100 MiB对象，cold/warm | c1/4/8/16/32/64 | 多人读取同一对象时，Page Cache、NIC、sendfile和下载槽谁先饱和？ |
| R2 Hot Chunk | 同一4 MiB Chunk，cold/warm | c1/4/8/16/32/64 | 热点Chunk是否可被大量独立连接稳定读取？ |
| R3 Random Objects | 64 KiB/1 MiB/16 MiB随机对象 | c1/4/8/16/32 | 不同对象、目录/Manifest读取与数据读取是否互相干扰？ |
| R4 Mixed Object Size | thumbnail + preview + original | 固定比例并发 | 大对象是否挤压小对象P95/P99？ |
| R5 Slow Reader Isolation | 正常读者 + 限速/暂停读者 | 逐档增加慢读者 | output buffer、下载槽、RSS和正常请求P99是否有上限？ |
| R6 Keep-Alive | keep-alive 对照 connection-close；64/150 KiB | c1/4/8/16/32 | 连接复用能否降低小对象QPS和尾延迟成本？ |
| R7 Real 3-host | R1/R3/R4的代表组，RF=2 | 真实三主机 | 独立磁盘、NIC/Tailscale下的结论是否仍成立？ |

截至 2026-08-30 的执行状态：R1/R2/R3/R4/R6 及 R5 的 loopback 首轮均已完成；R5 在真实 NIC/RTT/限速网络的资源
边界、严格冷缓存和 R7 三主机仍是发布阻塞项。SDK 后的单机结果与同日 MinIO 对照记录在
[SDK 性能验证与 MinIO 对照报告](MINIDRIVER_3_0_SDK_PERFORMANCE_AND_MINIO_COMPARISON_REPORT_2026-08-29.md)。这些 loopback
数据用于找本机拐点，不能替代 R7 的真实网络结论。

### 7.3 必做指标

```text
aggregate MiB/s + object QPS
P50/P95/P99 + success/503/retry
TCP连接数、建立数、Keep-Alive复用率、每连接请求数和关闭原因
active download lease、output buffer、BlockPool、RSS
Disk queue/pwritev/fdatasync（混合读写时）
Range实际读取字节/完整对象字节
最终SHA-256（完整对象或拼接Range）
```

### 7.4 任务与验收

- [x] Benchmark增加 `hot-object`（4 MiB即`hot-chunk`）、独立随机对象、`mixed-size` 与重复小对象 Keep-Alive profile；
- [x] 增加 `slow-reader` profile：限速客户端与正常客户端并发，并采集服务端 RSS、active download、output-buffer
  快照；loopback 的内核 socket buffer 会掩盖真实慢网络背压，故仍需 R7 复验；
- [x] 保留 connection-close 对照；报告单独记录 Keep-Alive 的 DataNode 连接建立/请求/复用次数；
- [ ] 小对象拆分 Session/Route/Body/Commit 与连接建立固定成本；
- [x] R4的fixture分配按轮次轮转；即使并发低于对象类别数，完整多轮也覆盖每一种大小；
- [ ] R4/R5至少持续30分钟，确认队列、output buffer和RSS不无界增长；
- [ ] R7使用独立磁盘和真实NIC/Tailscale；loopback只保留为开发回归；
- [ ] 将结果作为 LensCompute 第一版容量参数，不预先承诺跨硬件 SLA。

**验收：** 对“同对象热点、同Chunk热点、随机对象、大小混合、慢读者、Keep-Alive”各给出可复跑的容量拐点和资源上界；
若某组未能严格控制冷缓存，必须明确报告限制，不能把热读结果宣传为磁盘读性能。

## 8. Gate 5：基础全链路可观测性

### 8.1 统一关联字段

```text
requestId
sessionId
objectId/objectVersion
chunkId/generation
leaseId
nodeId
```

### 8.2 分段时间

```text
socket/body receive
checksum
disk queue
pwritev
group queue wait
fdatasync/index sync
replica connect/writable wait/durable ACK
Gateway Chunk/File Commit
```

### 8.3 任务

- [x] `requestId`贯穿Gateway、Primary、Replica和Chunk Commit控制请求；
- [ ] ReplicaUploadPipe记录连接复用、pending bytes、writable wait和ACK wait；
- [ ] 启动日志和benchmark记录`buildVersion/configDigest/durabilityMode`；
- [ ] 所有队列同时输出当前值、峰值和拒绝次数；
- [ ] 保存原始JSON/CSV，不能只在Markdown中抄最终均值。

2026-08-30 的最小 durability 矩阵已将 `buffered`、`group_commit` 和 `chunk_sync` 置于同一 SDK/新 SSD/单
DataNode 口径，原始 CSV 位于 `/data-ssd/minidriver-v3-durability-sdk-20260830/`。它证明每条 ACK 路径可运行并展示
同步成本，但每档仅一轮，不能替代 crash/recovery 或发布级容量结论。

不要求3.0建设完整Prometheus/Grafana平台；结构化日志和可复跑汇总脚本即可。

## 9. Gate 6：最小Docker Compose

### 9.1 交付

```text
Dockerfile.minidriver
deploy/v3/compose.local.yaml
  gateway
  dn-1
  dn-2
  dn-3
```

每个DataNode必须使用独立volume。禁止多个容器共享DataNode数据目录。

### 9.2 健康语义

```text
/healthz
  进程/EventLoop仍存活

/readyz
  数据目录可用
  未ENOSPC/只读
  durability worker正常
  admission允许新请求
```

### 9.3 任务

- [x] 多阶段Release镜像已写入 `Dockerfile.minidriver`；
- [x] `deploy/v3/compose.local.yaml` 已显式声明 Gateway、3 个 DataNode、端口、独立 named volume、健康检查和
  `opaque-chunk-id + crc32c` 默认协议；
- [~] 已通过 `docker compose ... config` 解析验证；实际 `build/up/readiness/smoke/down` 尚待 Docker daemon 可访问
  `ubuntu:22.04`。2026-08-30 的首次启动尝试被 daemon 配置的 `127.0.0.1:19999` 代理拒绝，不能误记为镜像/Compose
  已成功运行；
- [ ] `down`默认不删除volume；
- [ ] 容器重启后durable对象可读；
- [ ] ENOSPC、只读目录、Replica不可达和优雅停止测试；
- [ ] 记录`memory.current/memory.events/io.stat`和fd限制；
- [ ] 不引入Kubernetes、Helm和复杂Service Discovery。

## 10. Gate 7：真实三主机RF=2验收

### 10.1 拓扑

```text
S1：Gateway + DN-1
S2：DN-2
S3：DN-3
```

所有DataNode使用独立磁盘目录，通过真实NIC/Tailscale地址通信，禁止填写`127.0.0.1`或Docker bridge地址作为跨机
advertise address。

### 10.2 验收矩阵

| 场景 | 必须成立 |
|---|---|
| RF=2上传 | Primary和Replica均durable后Commit |
| Keep-Alive读取 | 连续请求正确且连接明显复用 |
| Range读取 | 206/416正确，拼接后SHA与原范围一致 |
| Mixed workload | 成功率、P95/P99、RSS和队列有记录 |
| Replica断开 | 请求失败/退避有界，不产生错误Commit |
| DataNode重启 | 已durable对象仍可读取 |
| ENOSPC | readiness降级，新写被拒绝，旧读不被伪装成功 |
| 30分钟持续负载 | RSS、BlockPool、pending queue不无界增长 |

最终形成`MINIDRIVER_3_0_RELEASE_REPORT_YYYY-MM-DD.md`，保存主机、磁盘、网络、revision、配置、原始结果和失败时间线。

## 11. 三轮实施顺序

### Round 1：语义与Worker读取边界

```text
Gate 1 immutable/version收口
Gate 1.5 SDK/integrity contract + C++ core MVP
Gate 2 Read Capability/read plan
Gate 5 requestId最小链路
```

停止条件：对象身份在重试、Commit、Event和Manifest中不一致，SDK 无法稳定复用原 chunkId，或 Worker 能够绕过
Capability读取任意Chunk。

### Round 2：批量读取和真实AI负载

```text
Gate 3 Keep-Alive
Gate 3 verified Chunk Range / segment sidecar
Gate 4 / P6-READ concurrent-read benchmark（R1～R6；R5 为慢读隔离）
Gate 5 Replica/连接指标
```

停止条件：连接复用导致响应串包、Range 的 verified 状态错误、慢客户端造成无界缓存，或小对象负载出现不可解释失败。

### Round 3：部署与冻结

```text
Gate 6 Docker Compose
Gate 7 三主机RF=2
restart/ENOSPC/Replica故障
3.0 Release Report
```

停止条件：容器重启后durable对象丢失、多个DataNode共享volume、跨机测试仍使用loopback，或故障产生错误Commit。

## 12. 明确延期清单

以下内容不再阻塞3.0：

```text
硬件加速CRC32C（当前软件 Provider 可先正确运行）
异步content fingerprint 的批量回填与收益实验
跨 tenant 精确去重、引用计数 GC 和长期收益实验
动态Group Commit控制器
multipart Range
对象原地覆盖与多版本保留
io_uring/splice/QUIC
Raft/Repair/Read Failover
多Gateway/LB
Kubernetes/Helm
Ray/LensCompute调度器
Replica-aware read scheduling
DataLocalityHint / Object placement affinity / storage-aware compute scheduling
```

发现新优化点时先记入Backlog。除非它破坏7个Gate之一，否则不重新打开3.0范围。

## 13. 3.0冻结后的项目关系

```text
MiniDriver 3.0 FREEZE
  → MiniDriver Client SDK（数据访问面）
  → LensGrid Go Backend
  → LensCompute Go Scheduler（计算控制面）
  → CPU Thumbnail/Preview
  → GPU OpenCLIP/YOLO/Denoise
  → 用真实AI workload反馈MiniDriver下一轮瓶颈

MiniDriver 4.0
  → Raft/Health/Fencing/Repair
  → 薄Gateway与多Gateway/LB
```

LensCompute发现的小对象、Range、热点或副本问题必须用可复现指标反馈；不能因为单次任务慢就直接把计算调度、模型或
业务状态重新耦合进MiniDriver。

## 14. 最终Definition of Done

只有以下全部成立，才发布并冻结MiniDriver 3.0：

```text
Gate 1：immutable object/version通过
Gate 1.5：SDK 上传/读取、CRC/strong-content 语义与兼容矩阵通过
Gate 2：Worker受控直读通过
Gate 3：Keep-Alive与可观察 verified/unverified 单Chunk Range通过
Gate 4 / P6-READ：R1/R2/R3/R4/R6读侧矩阵通过；R5慢读隔离有资源上界；R7三主机代表组通过
Gate 5：关键阶段可关联、可解释
Gate 6：Compose一键启动、重启和ENOSPC通过
Gate 7：真实三主机RF=2验收和Release Report完成
```

“冻结”表示停止无指标驱动的存储优化，不表示项目永久不再修改。LensCompute真实负载或4.0高可用设计发现明确瓶颈时，
再以新的版本和基线开启下一轮工作。
