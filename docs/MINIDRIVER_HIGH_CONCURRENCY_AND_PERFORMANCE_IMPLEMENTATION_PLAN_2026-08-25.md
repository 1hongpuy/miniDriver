# MiniDriver 3.0：高并发、高性能与持久化数据面完整实施设计（2026-08-25）

> **当前3.0发布执行入口：**
> [MiniDriver 3.0 AI-Ready对象存储冻结设计与实施计划](MINIDRIVER_3_0_AI_READY_RELEASE_PLAN_2026-08-28.md)。
> 本文继续保存完整性能实验、实现原理和长期优化Backlog；若任务优先级冲突，以AI-Ready发布Gate为准。

> 状态：3.0 主实施文档；性能基线、P2 持久化基线、P3 聚合写、P4 Group Commit、P5-A 协议契约与 P5-B
> 写入协调边界已完成；P5-C 双身份/可替换校验已完成正确性与首轮性能 A/B；P5-D 已完成 Session 预分配对象身份、
> Gateway 重启重试复用 Chunk 身份与 Commit 版本一致性。3.0 现已冻结为不可变对象且不实现覆盖，下一步按AI-Ready
> Gate补拒绝测试、Worker Read Capability、Keep-Alive与Range。
> 版本决定：3.0 先完成高性能数据面、明确落盘、协议边界与 Docker 可复跑部署；已有 MetadataStateMachine 成果保留，
> 三节点 Raft/Repair/多 Gateway 不作为 3.0 发布门槛，统一进入 MiniDriver 4.0 高可用主线。本文后续历史章节中的
> “3.1/3.2”版本号以新的4.0边界为准，不再作为当前执行阶段。
>
> 范围：Gateway/DataNode 的对象传输、身份与校验解耦、协议适配层、写入准入、背压、磁盘聚合写、副本队列、下载和
> 性能观测、持久化 ACK 和 Compose 运维。
> 不在范围：在没有替代校验与迁移方案时直接删除 SHA-256/副本确认；生产 Raft、Repair、AI GPU 推理、Kafka、
> Kubernetes、HTTP/2、QUIC、io_uring 的提前引入。

## 1. 目标与核心结论

MiniDriver 的性能目标不是“接收无限并发”，而是：

```text
负载低时：上传/下载路径高效，减少不必要拷贝、建连和阻塞。
负载升高时：资源有界、排队可观测、P95/P99 可解释、过载请求可重试。
节点故障时：V3 控制面正确性不让性能优化破坏对象或副本真相。
```

因此，正确的成功标准不是单次 MiB/s 最大值，而是：

```text
aggregate throughput + success rate + P50/P95/P99
+ queue/pause/RSS upper bound + version/checksum correctness
```

3.0 先把单节点和 RF=2 数据路径的真实性能、持久化和资源边界做扎实。已有 V3 MetadataStateMachine 代码继续保留并
通过测试，但 3.0 不同时接入生产 Raft/Repair，避免无法判断长尾来自磁盘、网络、副本还是共识。3.1 接入 HA 时必须复用
本版本建立的接口和性能门禁，不重新改写 Chunk Body 路径。

### 1.1 3.0 版本边界

```text
MiniDriver 3.0
  1 Gateway
  1～3 DataNode
  RF=1 性能隔离测试 / RF=2 正常对象语义
  HTTP/1.1 外部兼容 API
  HTTP Adapter → ChunkWriteCoordinator → ChunkStore / ReplicaTransport
  buffered / chunk-sync / group-commit 三种显式持久化模式
  单机与多进程 Docker Compose

MiniDriver 3.1
  3 Metadata/Raft + Health/Repair + fencing

MiniDriver 3.2
  多 Gateway + LB + 用户系统 + k3s/Kubernetes 生产化实验
```

代码中的 `/api/v2`、`MINIKV_V4_*` 等历史兼容名称不会在第一轮机械式全量改名。3.0 新配置统一使用
`MINIKV_V3_*`，读取时可暂时兼容旧变量；等数据格式、压测和回滚路径稳定后再清理名称，避免把无价值的大范围 rename
混入磁盘语义修改。

### 1.2 3.0 完成定义

| 维度 | 3.0 必须成立 |
|---|---|
| 正确性 | 上传、RF=2、下载、重试、删除与 SHA/checksum 校验全部通过；旧对象仍可读取 |
| 持久化 | `buffered/chunk_sync/group_commit` 语义可区分；durable ACK 必须晚于 Data 与 Index sync |
| 大对象写 | 4 MiB Chunk 的 Disk task 从当前约 80～90 个降到目标不高于 20 个，且吞吐/长尾至少一项稳定改善 |
| 并发 | 基准默认 4 上传、8 下载、4+4 混合不回归；更高并发走有界排队或可重试拒绝 |
| 内存 | BlockPool、write batch、replica pending、durability pending 都有硬上限；持续压测 RSS 不无界增长 |
| 小对象 | Session/Route/连接固定成本有独立数据；Keep-Alive 与批量 Route 的收益经过 A/B，而非凭经验启用 |
| 可观测 | 能拆出 receive/hash/disk queue/write/sync/index/replica/commit 各阶段 P50/P95/P99 |
| 部署 | 原生进程和 Docker Compose 使用同一配置契约，数据 volume、日志、健康检查和清理边界明确 |
| 报告 | buffered 与 durable、RF=1 与 RF=2、热读与冷读、loopback 与跨机结果不得混排 |

当前 `234.75 MiB/s` 单 DataNode buffered 峰值和约 `177.54 MiB/s` 同盘 RF=2 持续逻辑写是比较基线，不是 3.0
预先承诺的 SLA。持久化模式必须先得到首轮数据，再以相同语义和相同硬件评估与 MinIO 的差距。

## 2. 当前已实现的数据面基础

```text
Browser/Client
  → Gateway：预检、Session、Route、Commit（不转发 Body）
  → DataNode primary：流式 Chunk
  → DataNode replica：链式副本
```

当前代码已经具备：

| 层 | 已有机制 | 解决的问题 |
|---|---|---|
| 网络 | Multi-Reactor、EventLoopThreadPool、HTTP Keep-Alive | 多连接不由单 EventLoop 串行处理 |
| 上传 | Client 直传 DataNode | Gateway 不承载 4 MiB Chunk Body |
| 内存 | 64 KiB `SharedBodyBlock`、有界 BlockPool | 本地写盘和副本共享 Body，避免无限缓存 |
| 磁盘 | `ChunkDiskWritePipeline`、`DiskWriteExecutor`、`FastDataStore` | 将慢 `pwrite` 移离 I/O 回调，限制队列 |
| 背压 | 高/低水位线、`pauseRead()/resumeRead()` | 磁盘/副本慢时停止继续读 socket |
| 副本 | `ReplicaUploadPipe`、连接池、下游 ACK | 避免主节点无限堆积副本 Body |
| 准入 | `NodeResourceGovernor`、`maxActiveUploads`、503/Retry-After | 节点过载时可控拒绝而非 OOM |
| 下载 | extent + `sendfile` 路径 | 避免把大文件完整复制到用户态输出 buffer |
| 校验 | Chunk SHA-256、长度与副本确认 | 性能优化不牺牲正确性 |

这说明项目已经不是“只有 HTTP 上传”的原型；当前主要问题是容量边界与尾延迟，而非从零开始添加并发。

### 2.1 新目标：身份、版本、校验和去重分层

当前 `chunkHash` 同时承担 Chunk 身份、Gateway Route key、DataNode physical-index key、上传前去重键和完整性校验值。
这迫使 Browser、Primary DataNode 和 Replica DataNode 在 RF=2 的一次上传中至少执行三遍完整 SHA-256，并把某一种
Hash 算法固化进路由协议。

目标结构改为：

```cpp
enum class ChecksumType { kCrc32c, kSha256, kBlake3 };

struct ChecksumRecord {
    ChecksumType type = ChecksumType::kCrc32c;
    uint32_t segmentBytes = 0;       // 0 表示整个 Chunk；非 0 表示分段校验粒度
    std::string wholeDigest;         // 可选的整个 Chunk 强摘要
    std::string sidecarId;           // 可选：分段 checksum 列表/sidecar 的位置
};

struct ObjectRecord {
    std::string objectId;            // 稳定的逻辑对象身份
    uint64_t currentVersion = 0;     // 当前内容版本
    uint64_t metadataVersion = 0;    // 标签、权限等元数据版本
};

struct ObjectVersionRecord {
    std::string objectId;
    uint64_t objectVersion = 0;
    std::string manifestId;
    std::string contentHash;         // 可选；允许 Commit 后异步生成
};

struct ChunkRecord {
    std::string chunkId;             // 服务端生成的不透明身份，不再等于内容 Hash
    std::string objectId;
    uint64_t objectVersion = 0;
    uint32_t chunkIndex = 0;
    uint64_t size = 0;
    ChecksumRecord checksum;         // 传输/存储完整性；与写聚合大小相互独立
    std::string contentHash;         // 可选的精确去重指纹
};

struct ReplicaRecord {
    std::string chunkId;
    std::string nodeId;
    uint64_t nodeEpoch = 0;
    uint64_t generation = 0;
    std::string state;
};
```

核心关系变为：

```text
objectId + objectVersion
  → ordered chunkId
  → checksum（完整性）
  → optional contentHash（精确去重）
  → generation + replicas（路由和 fencing）
```

`objectVersion` 是业务提交顺序，不能由 Hash 代替：两个版本可以拥有相同内容 Hash；只改标签时只增加
`metadataVersion`。精确字节去重仍是 MiniDriver 的可选存储能力，但应从同步路由主键降级为可测量、可关闭、默认
tenant-scoped 的后台优化。miniAI 的 pHash/Embedding 只负责近似重复候选，不拥有物理引用合并或删除权限。

还必须区分三个独立参数：

```text
storageChunkBytes：   例如 4 MiB，决定路由、副本和 Commit 单位；
checksumSegmentBytes：例如 64/256 KiB，决定局部损坏检测与 Range Read 校验粒度；
targetBatchBytes：    例如 256 KiB，只决定一次 Disk task/syscall 聚合多少连续数据。
```

三者可以暂时取相近值，但数据模型和代码不能假设它们永远相等。

迁移期间同时支持两种存储键：

```text
legacy：cas:{chunkHash}
target：chunk:{chunkId}
```

旧对象不原地改写；Manifest/PhysicalStore 通过显式 `identityScheme` 读取对应格式。

### 2.2 网络协议与 Chunk 业务解耦

当前 HTTP Handler 直接解析 `X-Upload-Token`、`X-Replica-Chain`、URL `chunkHash`，随后创建磁盘 Pipeline 和副本
HTTP Pipe。目标不是立即把 HTTP/1.1 换成 gRPC，而是让 HTTP 只成为一个 Adapter：

```cpp
struct ChunkWriteDescriptor {
    std::string chunkId;
    std::string objectId;
    uint64_t objectVersion = 0;
    uint32_t chunkIndex = 0;
    uint64_t contentLength = 0;
    uint64_t generation = 0;
    std::vector<std::string> replicaChain;
    std::string capabilityId;
};

class ChunkWriteSink {
public:
    virtual BeginResult begin(const ChunkWriteDescriptor&) = 0;
    virtual PushResult push(SharedBodyBlock block, size_t offset, size_t length) = 0;
    virtual void finish(CompletionCallback) = 0;
    virtual void abort(std::string reason) = 0;
};

class ReplicaTransport {
public:
    virtual ReplicaStream open(const ChunkWriteDescriptor&, CompletionCallback) = 0;
};

class ChunkCommitReporter {
public:
    virtual void report(const ChunkCommitResult&, CompletionCallback) = 0;
};
```

边界如下：

```text
HttpChunkUploadAdapter
  负责：HTTP URL/Header/Content-Length/CORS/Capability 解码
  输出：ChunkWriteDescriptor + SharedBodyBlock stream

ChunkWriteCoordinator
  负责：本地写、副本写、背压、finish/abort、成功节点集合
  不知道：Header 名、HTTP status、TCP connection

ChunkStore
  负责：extent 分配、聚合写、checksum、finalize
  不知道：HTTP、浏览器、Replica URL

HttpReplicaTransport（当前实现）
  负责：把 ReplicaStream 映射为 HTTP/1.1 PUT
```

这样以后即使增加内部二进制 RPC、QUIC 或测试用内存 Transport，也不改 `ChunkStore` 和 Commit 规则。第一阶段必须
保留现有 HTTP/1.1 外部 API、pause/resume 与 SharedBlock 生命周期，不以“解耦”为名重写已验证的 Reactor。

### 2.3 磁盘聚合写：按逻辑 Chunk 和字节阈值，不按网络包计数

一个当前上传请求只描述一个逻辑 Chunk：`chunkId + Content-Length + Capability` 在 Body 到达前已经确定。TCP 分段、
HTTP parser 每次交付 20 KiB、47 KiB 或 64 KiB 都只是该 Chunk 的片段，不能用网络回调次数判断“对方发送了几个
Chunk”。HTTP/1.1 下多个 Chunk 是多个独立请求；未来若允许一个请求承载多个 Chunk，必须增加显式 frame header，
不能依赖 TCP 包边界。

第一版在每个 `ChunkWriteSession` 内维护独立的 `PendingWriteBatch`：

```cpp
struct BlockSlice {
    SharedBodyBlock block;
    size_t offset = 0;
    size_t length = 0;
};

struct PendingWriteBatch {
    uint64_t fileOffset = 0;         // 同一 extent 内必须连续
    size_t bufferedBytes = 0;
    std::vector<BlockSlice> slices;
    TimePoint firstBufferedAt;
};
```

收到任意片段时只累计 `bufferedBytes`。满足任一条件即 flush：

```text
1. bufferedBytes >= targetBatchBytes；
2. 已收到该 HTTP message 的完整 Content-Length / finishInput；
3. oldest buffered byte 等待超过 maxBatchDelay；
4. BlockPool、单流或全局 pending bytes 达高水位，需要释放内存；
5. abort/error 前丢弃未提交 batch；durable ACK 前强制 flush 并按策略 sync。
```

因此无论对方一次回调送来 256 KiB，还是六次合计 256 KiB，落盘调度都可以形成同一个聚合任务；最后一个不足阈值的
Chunk 尾部由 `finishInput` 立即提交，不会永远等待“凑满四块”。慢客户端由 `maxBatchDelay` 限制额外等待。

Primary 的本地磁盘聚合与副本网络发送必须彼此独立：Primary 可以把收到的 SharedBlock 立即交给
`ReplicaTransport`，同时在本地保留引用等待凑成 `PendingWriteBatch`；Replica 到达后再按自己的磁盘状态聚合。不要为了
本地 `256 KiB` pwrite 强制副本网络也等待 256 KiB，否则会增加首包延迟并扩大 SharedBlock 生命周期。网络侧是否使用
`writev/sendmsg` 合并 syscall 是另一个单变量实验。

当前 64 KiB 是 BlockPool 单块上限，而实测平均有效片段约 47 KiB，所以调参必须按字节而不是“块数”。第一轮候选：

```text
targetBatchBytes：64 / 128 / 256 / 512 KiB
maxBatchDelay：   0 / 0.5 / 1 / 2 ms
Disk Worker：     2 / 4（只做单变量组合）
```

推荐从 `256 KiB + 1 ms` 开始做 A/B，而不是直接写成生产默认。实现顺序先比较：

```text
A：现有每片段一个 SHA+pwrite task
B：同一 Chunk 的多个 BlockSlice → 一个 DiskExecutor task + pwritev
C：复制到连续 256 KiB buffer → 一次 pwrite
```

`pwritev` 可以在不额外 memcpy 的情况下提交多个不等长 slice；必须正确处理 partial write 并推进 iovec。第一版禁止把
不同 Chunk、不同 extent 的片段拼成同一个 `pwritev`，因为这会混淆失败、checksum、取消和 Commit 边界。跨 Chunk 的
磁盘并行仍由有界 `DiskWriteExecutor` 调度。

这里的“聚合写”也不等于持久化：普通 `pwrite/pwritev` 完成仍可能只进入 page cache。是否在 Chunk、文件或一组文件
执行 `fdatasync`，必须作为独立 durability policy 和 group-commit 实验，不能用更大 batch 冒充已经掉电持久化。

### 2.4 落盘语义：Data、Index 与 ACK 必须有明确顺序

当前实现的真实性边界是：

```text
disk0.data                 open(O_CREAT | O_RDWR)，普通 pwrite，无 fdatasync/fsync
DataNode physical_index    LevelDB WriteOptions 默认 sync=false
Gateway Session/Route/File LevelDB WriteOptions 默认 sync=false
```

因此当前 Chunk 成功表示“长度与 SHA-256 正确，数据和索引已交给内核/LevelDB”，不表示主机断电后一定可恢复。进程单独
崩溃时 page cache 通常仍由内核继续回写，但 guest OS、宿主机、虚拟磁盘或电源故障可能丢失尚未稳定写入的数据。

#### 2.4.1 内核为什么会自动写盘，应用还能控制什么

普通 buffered I/O 的实际路径是：

```text
MiniDriver SharedBodyBlock
  → pwrite/pwritev 把数据复制/映射进 Linux page cache
  → 对应页被标记为 Dirty
  → 内核 flusher 在后台选择时机提交 block I/O
  → guest 虚拟块设备
  → Hyper-V/VHDX 与宿主机缓存
  → SSD 控制器/介质
```

page cache 不是磁盘上的一块特殊空间，而是 Linux 借用的系统内存。应用在 `pwrite` 返回后可以释放
`SharedBodyBlock`，但内核中的 Dirty page 仍占用内存，直到写回完成。它不是无限的：

```text
dirty_background_bytes / dirty_background_ratio
  达到后台阈值：内核 flusher 开始异步 writeback；写进程通常还能继续。

dirty_bytes / dirty_ratio
  达到较高阈值：产生脏页的写进程会被节流，并参与/等待 writeback。

dirty_expire_centisecs / dirty_writeback_centisecs
  控制老脏页和周期性回写行为；具体值属于主机配置，不写死进应用协议。
```

因此应用不能要求内核“这 8 MiB 一定留在内存中，等我下令才写盘”，内核可因阈值、时间或内存压力提前回写。应用真正
可以控制的是：

| 动作 | 能控制什么 | 不能证明什么 |
|---|---|---|
| 普通 `pwrite/pwritev` | 提交数据并利用 page cache 合并/调度 | 返回时不证明断电可恢复 |
| `fdatasync(fd)` | 等待该文件需要的数据和必要元数据达到设备报告的稳定状态 | 不替代 Gateway/Metadata 自身持久化 |
| `sync_file_range` 等 hint | 提前启动某个范围的 writeback，减少稍后 sync 的脏数据量 | 它不是 durable barrier，不能据此 ACK |
| `O_DSYNC` | 让每次写带同步完成语义 | 会失去跨 Chunk 合并 sync 的机会 |
| `O_DIRECT` | 绕过 page cache，减少双缓存或缓存污染 | 不天然等于介质已持久化，且引入对齐/尾块复杂度 |

所以本文所说的“惰性落盘”不应理解为“MiniDriver 管住内核，直到凑满才让它写 SSD”。准确名称是：

```text
Delayed Durable ACK + Group Commit

数据：pwrite 后内核何时开始 writeback 可以早于应用计划；
屏障：MiniDriver 最多等待很短的 bytes/items/time 窗口再调用 fdatasync；
确认：只有屏障和同步索引都成功后，等待该批次的 Chunk 才进入 DURABLE 并收到 ACK。
```

在当前 16 GiB 虚拟机里，guest page cache 之外还可能存在 VHDX/宿主机缓存。因此 `Dirty` 下降或块设备写入计数增加只说明
发生了 writeback，不等于已经得到完整掉电保证；`fdatasync` 的保证还依赖虚拟块设备正确转发 flush/FUA，以及宿主机和
SSD 对完成状态的实现。第一版以 Linux `fdatasync` 返回作为应用可获得的明确持久化边界，并通过 guest reboot/宿主机
故障实验记录虚拟化环境的真实能力。

必须区分以下状态：

```text
RECEIVED   Body 已收齐；可能仍在应用队列
WRITTEN    pwrite/pwritev 返回；通常只保证进入 page cache
DURABLE    data/checksum 已 fdatasync，物理索引 WAL 已同步
COMMITTED  Gateway/Metadata 已持久记录满足策略的 durable replicas
```

目标策略做成显式配置，而不是让 HTTP 200 隐含不清楚的含义：

```cpp
enum class DurabilityPolicy {
    kBuffered,          // 性能/开发基线；不承诺断电恢复
    kChunkSync,         // 每个 Chunk 数据同步后才 ACK
    kGroupCommit        // 多个已写 Chunk 共用一次 sync，完成后分别 ACK
};

struct DurableReplicaResult {
    std::string chunkId;
    std::string nodeId;
    uint64_t generation = 0;
    uint64_t durableSequence = 0;
    bool dataSynced = false;
    bool checksumSynced = false;
    bool indexSynced = false;
};
```

单个 DataNode 的正确发布顺序为：

```text
1. 分配 tentative extent，状态只在内存/可恢复 allocator journal 中为 WRITING；
2. pwritev 数据，生成 checksum sidecar/记录；
3. fdatasync(dataFd)，必要时同步 checksum sidecar；
4. physical_index 使用 LevelDB WriteBatch + WriteOptions.sync=true 发布 COMMITTED extent；
5. 返回 DurableReplicaResult；
6. Primary 收齐策略要求的 durable replica ACK 后，才向 Gateway/Metadata Commit；
7. Gateway/Metadata 自身 durable commit 后，才向客户端返回 durable success。
```

顺序不能反过来。如果先同步 Index、后同步 Data，断电后会出现“索引存在但 extent 内容未落盘”；如果 Data 已同步而
Index 尚未发布，最多形成可扫描/可回收的孤儿 extent，通常比暴露损坏对象更安全。allocator journal、启动时 orphan
scan 和 free-space rebuild 必须覆盖这类窗口。

当前所有 Chunk 共用一个 `disk0.data`，这反而适合第一版 group commit：每个完成 `pwritev` 的 Chunk 把 promise 放入
该 volume 的 `DurabilityCoordinator`，达到任一阈值后由一个磁盘线程执行一次 `fdatasync(dataFd)`，再用一个同步
LevelDB WriteBatch 发布本组 extent：

```text
groupCommitBytes：4 / 8 / 16 MiB
groupCommitDelay：1 / 2 / 5 ms
groupCommitItems：达到任一字节/数量/时间阈值即提交
```

#### 2.4.2 MiniDriver 的惰性持久化算法

每个 volume 独立维护一个有界的 Coordinator；它不缓存第二份文件 Body，只保存已经完成 `pwritev` 的小型 waiter/extent
元数据。真正尚未落盘的数据位于内核 Dirty page 中：

```cpp
struct DurableWaiter {
    uint64_t writeSequence = 0;
    uint64_t bytes = 0;
    ExtentRecord tentativeExtent;
    TimePoint writtenAt;
    CompletionCallback completion;
};

struct DurabilityBatch {
    uint64_t flushThroughSequence = 0;
    uint64_t bytes = 0;
    std::vector<DurableWaiter> waiters;
};
```

状态流程为：

```text
pwritev 完成
  → 在 Coordinator 锁内分配 writeSequence
  → waiter 进入 pending；此时只可标记 WRITTEN
  → bytes >= 8 MiB，或 items >= 8，或 oldest >= 2 ms
  → 截止当前 sequence 形成不可变 DurabilityBatch
  → fdatasync(disk0.data)
  → 成功：physical_index 用一个 sync=true WriteBatch 发布该组 extent
  → 成功：逐个 waiter 标记 DURABLE 并回调副本/Primary
  → 失败：整组返回 I/O error，节点进入写入降级/隔离，绝不发送 durable ACK
```

第一轮建议值只是实验起点：

```text
groupCommitBytes = 8 MiB
groupCommitItems = 8 Chunk
groupCommitDelay = 2 ms
maxPendingDurabilityBytes = 64 MiB / volume
maxPendingDurabilityItems = 64 / volume
maxConcurrentSync = 1 / volume
```

其中 `groupCommitDelay` 是允许增加到正常请求上的最大主动等待，不是承诺 sync 在 2 ms 内完成。若磁盘同步本身需要
20 ms，ACK 仍必须等这 20 ms。达到任一 pending 高水位后停止接收新的前台 Body或触发准入拒绝，不能靠增加系统 Dirty
page 把慢盘隐藏成快盘。内核更早把本批数据写回并不会破坏算法，反而可能缩短后续 `fdatasync`；但 MiniDriver 仍要调用
barrier，取得明确成功或错误结果。

批次边界必须满足：

```text
只 ACK writeSequence <= flushThroughSequence 的 waiter；
flush 开始之后完成 pwrite 的 Chunk 进入下一批，即使它碰巧也被同一次内核 flush 写出；
Data sync 成功、Index sync 失败时不 ACK，启动恢复将其作为 orphan extent 处理；
Data sync 失败时不发布 Index，并保留错误用于节点 health/fencing；
进程关闭时先停止新 admission，再 drain 或明确失败 pending waiters，不能静默退出。
```

应用层还要观测内核而不是替内核重新实现 VM 策略：

```text
/proc/meminfo：Dirty / Writeback
/proc/vmstat：nr_dirty / nr_writeback / 写回与节流相关计数
DataNode：pending durability bytes/items、group wait、sync latency/error
设备：吞吐、await、queue depth、utilization
容器：memory.current / memory.events / io.stat（部署到 cgroup v2 后）
```

第一版不自动修改全局 `vm.dirty_*`。这些参数影响整台机器上的 PostgreSQL、LevelDB、日志和其他服务；先在隔离测试机记录
现值并测量。如果持续写入在内核高阈值处形成周期性百毫秒长尾，再把固定 `dirty_*_bytes` 作为部署级 A/B 参数，而不是
由 DataNode 进程运行时偷偷改 sysctl。

当前开发虚拟机在 2026-08-27 的只读快照为：

```text
MemTotal / MemAvailable       15.58 GiB / 13.19 GiB
vm.dirty_background_ratio    10%
vm.dirty_ratio               20%
vm.dirty_expire_centisecs     3000（约 30 秒）
vm.dirty_writeback_centisecs   500（约 5 秒）
采样瞬间 Dirty / Writeback    292 KiB / 0 KiB
```

ratio 的实际计算基数是内核认定的可用/可回收内存，不严格等于 `MemTotal`；按该次 `MemAvailable` 只能粗略理解为约
`1.3 GiB` 开始后台回写、约 `2.6 GiB` 量级会强力节流写入者。这个缓冲足以让几秒钟的短压测先获得很高吞吐，持续写入后
再暴露真实 SSD/writeback 速度。`Cached` 即使显示数 GiB，也包含大量已落盘、可回收的 clean page，不能把它全部当成
“尚未落盘的数据”。

一次 `fdatasync` 会覆盖该文件在调用前的相关脏数据，所以可以唤醒本组多个 Chunk waiter；但必须以单调
`durableSequence` 标记 flush 边界，不能把 flush 开始后才写入的数据错误地算入旧 ACK。每个物理 volume 使用独立
Coordinator，慢盘不能持有全局锁阻塞其他盘。

可用 `sync_file_range`/writeback hint 提前启动后台回写以缩短最后一次 `fdatasync`，但它本身不是 durability barrier。
`O_DIRECT` 只绕过 page cache，也不天然等于介质持久化；若选择 Direct I/O，还需要对齐缓冲、完整的尾块处理，以及
`O_DSYNC`/`fdatasync` 等明确完成语义。第一版优先保留 buffered I/O + group commit，避免同时引入对齐和文件系统差异。

RF=2 必须在 API 中说明哪一种副本策略才返回成功：

```text
buffered benchmark：       两个副本 WRITTEN，可丢电；只用于明确标记的性能基线
strict durable：           两个副本 DURABLE 后 File 才为 AVAILABLE
degraded/protecting：      只有一个 durable 副本时显式标记，不伪装成 RF=2 完成
```

性能报告必须分别列出，禁止把 `buffered/no-sync` 的 MiB/s 与 `group-commit` 或 `per-chunk fdatasync` 横向混成同一排名：

```text
Mode A：pwritev + no sync
Mode B：pwritev + per-Chunk fdatasync + LevelDB sync
Mode C：pwritev + group commit + batched LevelDB sync
```

除延迟/吞吐外还要记录：sync 次数、每次覆盖字节、sync P50/P95/P99、等待 group commit 的时间、durable waiter 数、
启动恢复后的 orphan/invalid extent 数，以及 kill -9、guest reboot、宿主断电等不同故障模型。普通 `kill -9` 只能证明
进程恢复，不能替代整机掉电验收。

## 3. 当前已知瓶颈与证据

现有 V4.3 压测报告表明，在默认两写槽下系统较稳定；提高到四写槽在 upload-only 下可提高 aggregate throughput，
但 mixed c8 会放大写入尾延迟。已有一次独立观测为：

| 指标 | 观测值 | 解释 |
|---|---:|---|
| Chunk total P50 / P95 / P99 | 52 / 420 / 464 ms | 高负载下尾部排队明显 |
| pwrite P50 / P95 / P99 | 7 / 210 / 415 ms | 磁盘/页缓存抖动是主要证据 |
| Body receive P95 | 419 ms | pause 使上游接收延后，属于有意背压结果 |
| 最长 disk pause | 418 ms | 队列受到控制，但客户端尾延迟增加 |
| BlockPool 生命周期峰值 | 8 / 8 MiB | 内存上限确实发挥了限制作用 |
| I/O EventLoop 最大 timer lag | 约 3 ms | 不能把主要问题归因于 EventLoop 卡死 |

因此当前优先问题是：

```text
并发 pwrite / page cache 抖动
  → DiskWriteExecutor queue wait
  → pipeline pause
  → Browser/Client 的 Chunk 与文件 P95/P99 上升
  → 副本网络等待在高压下进一步放大
```

Gateway routes/commit 在已有本机测量中仍是微秒级，暂不是第一瓶颈；V3 的 Raft majority commit 会改变控制面
写延迟，必须作为单独指标测量，不能把它混入 DataNode Body 吞吐结论。

相关证据见 [V4.3 当前性能分析与优化路线](V4_3_CURRENT_PERFORMANCE_ANALYSIS_AND_OPTIMIZATION_ROADMAP_2026-08-12.md)
和 [四写槽容量报告](V4_3_FOUR_SLOT_CHUNK_CAPACITY_REPORT_2026-08-12.md)。这些结果来自特定机器与负载，
不应外推成所有部署的绝对性能承诺。

## 4. 性能模型：三个独立资源平面

### 4.1 控制面并发

```text
preflight / session / routes / chunk commit / file commit / heartbeat / manifest
```

主要资源：Gateway CPU、LevelDB/Raft、Metadata Leader、网络 RTT。V3 后所有一致性写都由 Leader 顺序提交并等待
多数派；这会提高单写延迟，但不应让 Chunk Body 经过 Raft。

### 4.2 数据面并发

```text
Client socket
  → primary receive/hash/write
  → primary → replica stream
  → replica write/hash/ACK
```

主要资源：NIC、CPU hash、I/O EventLoop、BlockPool、磁盘队列、副本连接。它决定大文件 aggregate MiB/s 与
Chunk P95/P99。

### 4.3 后台任务并发

```text
Repair / delete / thumbnail / AI object read
```

后台任务不能抢走所有磁盘、网络或前台 upload slot。Repair 在 V3 需要独立预算；AI Worker 必须使用受控读取、
并发限制和低优先级，不得使 DataNode 下载路径失控。

## 5. 资源预算与背压规则

每一种资源都必须有明确所有者、上限和满载行为：

| 资源 | 当前/目标所有者 | 上限示例 | 满载行为 |
|---|---|---|---|
| 浏览器文件并发 | Web client scheduler | 3～6 文件、每文件有限 Chunk window | 本地排队，公平轮转 |
| DataNode 前台写 | `NodeResourceGovernor` | `maxActiveUploads`，默认从 2 起测 | Gateway 不再路由 / 返回 503 |
| 单 Chunk 内存 | BlockPool + pipeline | 64 KiB Block、1 MiB high watermark | `pauseRead()` |
| 磁盘任务 | `DiskWriteExecutor` | worker 数与 queue bytes 有界 | 停止接收更多 Body |
| 副本发送 | `ReplicaUploadPipe` | pending bytes + 每 loop 连接池上限 | 暂停 primary 上游读取 |
| 下载 | Download lease / output watermark | 每节点下载槽和输出 buffer 上限 | 限流或可重试拒绝 |
| Repair | RepairCoordinator + DataNode | `maxRepairTasks` 独立且很小 | retry/backoff，不占前台写槽 |
| AI 读取 | CineLake worker | object read 并发/字节/超时 | Job 排队，不影响 Object COMMITTED |

背压链必须连续，不能只限制其中一个队列：

```text
disk or replica slow
  → local/downstream pending reaches high watermark
  → HTTP body callback returns pause
  → TcpConnection pauseRead
  → TCP receive window naturally applies pressure upstream
  → drain below low watermark
  → resumeRead
```

这比“读完全部请求 Body 再异步写盘”更能控制 RSS 与 P99。

## 6. 3.0 代码改动地图

| 文件/新模块 | 3.0 改动 | 兼容要求 |
|---|---|---|
| `FastDataStore` | durability policy、data sync、同步 Index 发布、sync 指标 | 默认 buffered 时行为等价；旧 physical index 可读 |
| `ChunkDiskWritePipeline` | 同一 Chunk 的 `PendingWriteBatch`、`pwritev`、尾块/超时 flush | 保留旧单 block 路径作为运行时回退 |
| `DiskWriteExecutor` | queue/work/batch/sync 指标与有界任务 | 不在 Worker 线程操作 HTTP connection |
| `DurabilityCoordinator`（新） | 每 volume group commit、sequence fence、waiter 上限、关闭 drain | 不持有 Body 副本；不跨 volume 共用全局锁 |
| `HttpChunkUploadAdapter`（新边界） | URL/Header/Capability → `ChunkWriteDescriptor` | `/api/v2` 与现有 Header 暂时保持 |
| `ChunkWriteCoordinator`（新边界） | 本地写、副本、背压、finish/abort、ACK 聚合 | 不知道 HTTP Header 和 status code |
| `ReplicaTransport` | 现有 HTTP/1.1 副本流成为一个实现 | SharedBlock 生命周期、pending 上限不回归 |
| `GatewayState/Metadata` | 批量 Route/Commit、对象版本、持久化状态字段 | 旧 `chunkHash` 对象双读，不原地改写 |
| Benchmark | Keep-Alive、durability mode、物理/逻辑字节、持续时间与故障测试 | 原始结果必须保存配置和版本 |
| Compose | Gateway/DataNode volume、healthcheck、ulimit、日志与环境变量 | 不使用临时容器层保存对象数据 |

3.0 新配置统一设计为：

```text
MINIKV_V3_DURABILITY_MODE=buffered|chunk_sync|group_commit
MINIKV_V3_WRITE_BATCH_BYTES=262144
MINIKV_V3_WRITE_BATCH_DELAY_US=1000
MINIKV_V3_GROUP_COMMIT_BYTES=8388608
MINIKV_V3_GROUP_COMMIT_ITEMS=8
MINIKV_V3_GROUP_COMMIT_DELAY_US=2000
MINIKV_V3_GROUP_COMMIT_MAX_PENDING_BYTES=67108864
MINIKV_V3_GROUP_COMMIT_MAX_PENDING_ITEMS=64
MINIKV_V3_DISK_WRITE_WORKERS=2
MINIKV_V3_DISK_WRITE_BLOCKS=128
```

所有配置都要经过范围校验并在启动日志打印最终值。持久化策略非法时启动失败，不能静默退回 buffered；性能调优参数非法
时可使用明确记录的安全默认值。

## 7. MiniDriver 3.0 分阶段实施任务

### P0：冻结性能与正确性基线（已完成本机部分）

- [x] 完成 upload-only、download-only、mixed 并发矩阵；
- [x] 完成 64 KiB/4/16/64/256 MiB 对象与端到端 SHA-256；
- [x] 完成 4/8/12/16 上传槽容量扫描，确定本机常规 4 上传、8 下载、4+4 mixed；
- [x] 完成新 SSD fio、MiniDriver RF=1/RF=2 与 MinIO/Warp 数量级对照；
- [x] 完成 DataNode on-CPU flame graph和分段日志定位；
- [ ] 补真实三主机网络基线；loopback 结果不得外推成跨机数字。

**验收：** 后续每个改动都能与同一脚本、对象集、持久化模式和硬件条件下的原始结果比较。

### P1：补齐观测与版本化配置（进行中）

- [x] `DiskWriteExecutor` 已记录 task queue wait、worker work、peak queue；
- [x] `FastDataStore` 已记录 SHA update/finalize、pwrite 与 index 时间；
- [x] DataNode 已支持 Disk Worker/Block 数量配置并打印运行参数；
- [x] 增加 write batch、sync、durable waiter 和 Group Commit 指标；内核 writeback 采集仍留给部署/压测脚本；
- [ ] 增加 `buildVersion/configDigest/durabilityMode` 到启动日志和 benchmark 结果；
- [ ] 给 `ReplicaUploadPipe` 增加连接复用、pending、writable wait 与 ACK wait；
- [ ] 将 Chunk requestId 贯穿 receive/disk/replica/commit 日志。

**验收：** P99 上升时能定位到 hash、disk queue、write、sync、replica、Gateway commit 或客户端排队中的一项。

### P2：持久化基线——先实现 buffered 与 per-Chunk sync（主体已完成）

- [x] 为 `FastDataStore` 增加显式 `DurabilityPolicy`，默认 `buffered` 保持兼容；
- [x] `chunk_sync` 按 data `fdatasync` → physical index `sync=true` → success 的顺序实现；
- [x] 单 Chunk 日志记录 data sync、index sync 次数、耗时与 durable 标志；
- [x] `MINIKV_V3_DURABILITY_MODE` 非法时拒绝启动；
- [x] 增加 buffered/chunk_sync、旧 buffered 对象重新验证与同步发布的 Store 单元测试；
- [x] 对 RF=1 运行相同 16 MiB c1/c4/c8/c16 与 64 MiB 持续写，得到首份 durable 基线；
- [ ] 将 DataNode durability 配置解析提取为可独立单测的配置模块；
- [x] 对 data sync EIO、data sync 成功但 index sync 失败建立可注入测试接口；ENOSPC 设备级测试留到 P7。

2026-08-27 新 SSD 首轮结果（1 Gateway + 1 DataNode、4 MiB Chunk、2 Disk Worker）已经证明两种模式不是只改了
配置名：`chunk_sync` 的可观测 Chunk 每个都执行一次 data `fdatasync` 和一次 LevelDB `sync=true`。64 MiB/c4 的 10 轮
持续逻辑吞吐从 `221.324 MiB/s` 降到 `143.576 MiB/s`，约下降 35.1%；文件 P95 从 `781.679 ms` 增至
`1921.542 ms`。`fdatasync` P50/P95/P99 为 `6.119/28.374/45.872 ms`，最大值达到 `615.538 ms`。完整结果见
[性能基线报告 6.8.12](MINIDRIVER_PERFORMANCE_BASELINE_AND_BOTTLENECK_REPORT_2026-08-25.md#6812-buffered-与-per-chunk-durable-基线2026-08-27)。

这组数据的用途不是把 per-Chunk sync 设成最终默认值，而是固定最严格、最容易解释的对照下界。P3 先减少每 Chunk
约 80～90 个小 Disk task；P4 再把每 Chunk 一次 data/index sync 合并为有界 Group Commit。

**停止条件：** Data 未同步就可见 Index、sync 失败仍返回 2xx、或默认 buffered 行为回归时，不进入 Group Commit。

### P3：256 KiB 聚合写与 `pwritev`（已完成）

- [x] 为每个 Upload Stream 增加独立 `PendingWriteBatch`，只聚合同一 Chunk、同一连续 extent；
- [x] 实现完整的 `pwritev` partial-write/iovec 推进，并用超过 `_SC_IOV_MAX` 的碎片测试多次推进与字节顺序；
- [x] 保留 `MINIKV_V3_WRITE_BATCH_MODE=single` 旧单片段 `pwrite` 回退路径；
- [x] 完成 64/128/256/512 KiB 和 0/1/2 ms 单变量矩阵；当前 EventLoop timer 粒度为 1 ms，0.5 ms 不伪装成独立档；
- [x] Body end、1 ms deadline、内存高水位、cancel/error 均能处理未满 batch；
- [x] 保持 SharedBlock no-copy `pwritev`；基于既有 memmove 火焰图与本轮验收收益，不再为 P3 增加 staging copy 分支；
- [x] 不等长碎片、尾块、慢到达 deadline、BlockPool 耗尽、single 回退、RF=2 Replica 数据链和最终 SHA 全部回归。

2026-08-27 新 SSD 结果：`256 KiB + 1 ms` 将 4 MiB Chunk 的 Disk batch P50/P95 从 single 的 `78/121` 降为
`16/18`。16 MiB/c8 buffered 聚合吞吐从 `227.528` 提高到 `246.998 MiB/s`（+8.6%），文件 P95 从
`409.995` 降到 `371.031 ms`（-9.5%）。持续 64 MiB/c4 buffered 吞吐只提高 1.7%，且 P95 有波动；chunk_sync
持续吞吐只提高 2.0%，证明 P3 已经解决“小 task/pwrite 次数”，但 durable 主瓶颈仍是每 Chunk 的 data/index sync。

完整 9 组 c8 + 4 组持续写为 `520/520` 成功；额外 RF=2、chunk_sync 端到端矩阵覆盖 64 KiB/4/16/64/256 MiB，
`36/36` 对象通过最终 SHA。原始结果与分析见
[性能基线报告 6.8.13](MINIDRIVER_PERFORMANCE_BASELINE_AND_BOTTLENECK_REPORT_2026-08-25.md#6813-p3-同-chunk-聚合写与-pwritev-验收2026-08-27)。

**验收：** 4 MiB Chunk 的 Disk task 目标不高于 20；SHA/checksum、RSS 和背压不回归，并在 CPU、吞吐或 P99 至少一项
获得可重复收益。

### P4：Group Commit 与真正 durable ACK（已完成）

- [x] 新增每 `FastDataStore`/volume 一个独立 `DurabilityCoordinator` 和 sync worker，不占用两个 Disk Worker；
- [x] 以 `bytes/items/delay` 任一阈值切批，并以单调 `writeSequence/durableSequence` fencing ACK；
- [x] 首测 `8 MiB / 8 items / 2 ms`，并比较 4/16 MiB 与 1/5 ms；
- [x] pending bytes/items 有硬上限；新 Chunk 在读取 Body 前走 admission，enqueue 仍做第二次硬边界校验；
- [x] data `fdatasync` 后用一个 LevelDB `WriteBatch(sync=true)` 原子发布该批 extent；
- [x] 实现 shutdown drain、data/index sync error、启动 orphan extent 扫描与 free extent 重建；
- [x] 分开报告 no-sync、per-Chunk sync、group-commit 三种成绩；
- [x] 暂不自动调整 `vm.dirty_*`；Dirty/Writeback 与设备 await/queue 保持为部署侧观测项。

2026-08-28 新 SSD 的 RF=1 矩阵全部成功。当前候选 `8 MiB / 8 items / 2 ms` 在 16 MiB/c8 下从 per-Chunk
`149.802 MiB/s` 提高到 `212.604 MiB/s`（+41.9%）；64 MiB/c4 持续写从 `153.435` 提高到
`181.138 MiB/s`（+18.1%），P95 从 `1331.507` 降到 `1048.976 ms`（-21.2%）。可观测的 798 个 Chunk 只执行
553 次 data/index sync，sync 次数下降约 30.7%。更大的 `16 MiB/16/2 ms` 确实进一步合批，但确认轮持续吞吐只有
`173.542 MiB/s`，所以没有替换 8 MiB 候选。

RF=2 smoke 覆盖 64 KiB/4/16/64 MiB，`32/32` 个对象完成 Primary→Replica、两端 durable ACK、File Commit、下载与
最终 SHA-256；两节点共记录 352 个 Chunk，未发现 `durable=false` 或错误 ACK。故障测试覆盖 data sync EIO、index
sync failure、shutdown drain、pending admission 和重启 orphan 回收。完整证据见
[性能基线报告 6.8.14](MINIDRIVER_PERFORMANCE_BASELINE_AND_BOTTLENECK_REPORT_2026-08-25.md#6814-p4-group-commit-与-durable-ack-验收2026-08-28)。

**验收：** group commit 相比 per-Chunk sync 显著减少 sync 次数；所有 durable waiter 有界；任何 ACK 都能关联到一个已成功
完成 data+index sync 的 `durableSequence`。

### P5：协议、身份、版本与校验解耦

- [x] 定义与 HTTP 无关的 `ChunkWriteDescriptor`、`ChunkIdentityScheme`、`ChunkChecksumType` 和 `ReplicaTarget`；
- [x] 增加 v2 Upload Capability，携带 `chunkId/objectId/objectVersion/generation/checksum`，同时继续验证 v1 token；
- [x] 抽出 `HttpChunkUploadAdapter`，把 URL/Header/Capability/Replica Chain 解码收敛为 Descriptor；
- [x] 将磁盘与副本流水线的背压结果改为协议无关的 `StreamConsumeResult`；只有 HTTP Adapter 映射为
  `HttpContext::BodyConsumeResult`；
- [x] Gateway 先以 `cas-sha256 + sha256` 兼容模式签发 v2 token，RF=2 行为保持不变；
- [x] 增加协议单测，并完成 64 KiB/4/16/64 MiB、`32/32` 对象的 RF=2 durable 回归；
- [x] 从现有 `ChunkUploadStream` 抽出不生成 HTTP response 的 `ChunkWriteCoordinator`，再定义
  `ChunkStore/ReplicaTransport/ChunkCommitReporter` 接口；
- [x] 3.0 的 Gateway Session/File/Object/Route/Capability/Manifest 已贯穿
  `objectId/objectVersion/metadataVersion/chunkId/generation/ChecksumType`；
- [ ] 4.0 接入 Raft 时同步升级 MetadataStateMachine record/command；它不再作为 3.0 数据面发布门槛；
- [x] `identityScheme=cas-sha256|opaque-chunk-id` 双读，旧对象不原地转换；
- [x] Route/Capability 迁到 `chunkId + objectVersion + generation`，但保留 `/v2/chunks/{chunkHash}` 兼容 Adapter；
- [ ] `ChecksumProvider` 先保持 SHA-256，再 A/B CRC32C 热路径、Primary 强 Hash 和异步 content fingerprint；
  Provider、CRC32C、RF=2 正确性和首轮性能 A/B 已完成，硬件加速 CRC32C、Primary 强 Hash 与异步强指纹仍待实现；
- [x] Session 创建时预分配并持久化逻辑 `objectId`；同一 Session 的 Route 重试以及 Gateway 重启后重试复用原
  `chunkId`，Commit 沿用同一 `objectId/objectVersion/metadataVersion`；
- [ ] 增加显式覆盖/新版本 API 后，验证相同内容的新对象版本不能被 Hash 隐式吞掉；3.0 当前只有新建路径且拒绝覆盖；
- [ ] 统计真实去重率和 SHA CPU ms/GiB，再决定同步精确去重是否默认开启。

**验收：** 切换 checksum 不改变路由身份；新旧对象均可上传、重试、Commit、下载和校验；HTTP 细节不再进入 ChunkStore。

#### P5-D Session/Object identity 检查点（2026-08-28）

3.0 的单 Gateway 路径不再等到 File Commit 才临时生成对象身份：Session 创建时即生成并持久化
`objectId + objectVersion + metadataVersion`，Route Capability 和API响应使用同一身份，File/派生对象 Commit 也沿用它。
旧的 10/11/13 字段 Session 记录继续按版本1读取；新记录使用16字段格式。

新增 `gateway_route_retry_identity` 覆盖 Gateway 关闭重开后的 Session 恢复、相同 opaque Route 重试、稳定 `chunkId` 和
最终 Object Commit 身份一致性。相关 Gateway 回归 `12/12` 通过；全量 CTest 中其余纯本地测试通过，依赖监听 Socket、
Redis 或 Playwright 的用例在当前受限执行环境中无法运行，不能把环境失败记为功能回归。

#### P5-A 实施检查点（2026-08-28）

当前完成的是兼容性地基，不等于整个 P5 已完成：

```text
Gateway v2 Capability
  → HttpChunkUploadAdapter
  → ChunkWriteDescriptor(cas-sha256, sha256, version, generation)
  → StreamConsumeResult（协议无关背压）
  → 现有 ChunkUploadStream / FastDataStore
```

新字段已经由 Gateway 签名、DataNode 验证并进入结构化完成日志；v1 token 仍归一化为同一个 Descriptor。为避免在
PhysicalStore 尚未支持双键时把不透明 `chunkId` 错当成 SHA-256，Adapter 会显式拒绝尚未启用的
`opaque-chunk-id` 和非 SHA Provider。这是迁移围栏，不是最终能力。下一步必须先把写入协调器从 HTTP response 生命周期中
分离，再开启 opaque 写入和 checksum A/B。

背压枚举迁移后，`ChunkDiskWritePipeline` 不再包含 `HttpContext.hpp`，`ReplicaUploadPipe` 的输入结果也不再泄漏 HTTP
parser 类型。HTTP/1.1 入口仅在最外层执行四个枚举值的一对一映射，`pause/pause-before-consume/abort` 语义未改变。

#### P5-B/C 实施检查点（2026-08-28）

P5-B 已把原来的 `ChunkUploadStream` 拆成两层：

```text
HttpChunkUploadStream
  只负责：HTTP decode、BodyConsumeResult 映射、DeferredResponse、CORS/JSON

ChunkWriteCoordinator
  负责：admission、本地 ChunkStore、ReplicaTransport、durable finish、Gateway commit
  返回：ChunkWriteResult，不生成 HttpResponse
```

新增的 `ChunkStore` 由 `FastDataStoreChunkStore` 实现；`ReplicaTransport` 当前由 `HttpReplicaTransport` 实现，只有该实现
知道 Replica PUT 的 URL/Header 和连接池。控制面提交继续使用既有协议无关的 `GatewayControlClient` 接口，当前实例是
`HttpGatewayControlClient`。因此未来增加内部二进制 Transport 时，不需要修改磁盘流水线、校验或 Commit 判定。

P5-C 已实现以下双格式：

```text
legacy physical index：e:{sha256}       + cas-sha256/SHA-256
target physical index：o:{chunkId}      + opaque-chunk-id/SHA-256 或 CRC32C
```

`FastDataStore` 启动恢复、读、写和删除都识别两种 key；旧记录不原地迁移。Gateway 的 Route、Capability、Manifest 与删除
任务同时保存逻辑 `chunkHash` 和物理 `storageIdentity`，避免 opaque 模式删除时误把内容 SHA 当物理键。浏览器和 benchmark
下载仍对最终内容执行 SHA-256，CRC32C 只替换 DataNode 热路径的传输/存储校验，不降低客户端端到端验证边界。

本轮验收：

```text
CTest（排除当前环境缺少 Playwright 的 UI 用例）：54/54
CAS + SHA-256，RF=2：64 KiB / 4 MiB / 16 MiB，12/12 端到端成功
opaque chunkId + CRC32C，RF=2：64 KiB / 4 MiB / 16 MiB，12/12 端到端成功
双格式存储单测：写入、校验失败、重启恢复、读取和删除均通过
```

复跑入口为 `tools/run_v3_p5_protocol_smoke.sh`。该轮是正确性 smoke，数据目录位于 `/tmp` 且采用 Group Commit，吞吐
数字不能与新 SSD 性能基线直接比较。

#### P5-C 性能 A/B 检查点（2026-08-28）

随后在 `/data-ssd/minidriver-v3-p5-ab-20260828` 固定 `opaque-chunk-id + RF=2 + group commit + 256 KiB pwritev`
完成 SHA-256/CRC32C 对照，五类负载共 `424/424` 条 benchmark 记录成功。CRC32C 将每 4 MiB Chunk 的 checksum
平均耗时降低约 19%～31%；16 MiB 纯上传吞吐提高约 2%～6%，64 MiB/c4 持续吞吐提高 7.3%，mixed 4+4 聚合吞吐
提高 10.1%。CRC32C 当前仍是软件 table 实现，不是硬件加速上限。

更关键的是，固定 8 Chunk budget 时继续把文件并发由 c4 增到 c8/c16，纯上传吞吐只小幅增加，P95 却大幅增长。
这证明 checksum 优化有价值，但 durable group wait、sync 和同盘 RF=2 仍决定平台；默认文件并发仍保持 4，而不是因为
CRC 更快就提高到 16。完整结果见
[性能基线报告 6.8.16](MINIDRIVER_PERFORMANCE_BASELINE_AND_BOTTLENECK_REPORT_2026-08-25.md#6816-p5-bcopaque-identity-与-sha-256crc32c-durable-ab2026-08-28)，
复跑入口为 `tools/run_v3_p5_checksum_ab_benchmark.sh`。

#### P5-C Group Commit delay 固定点（2026-08-28）

在 `opaque-chunk-id + CRC32C + RF=2 + 256 KiB pwritev + 8 MiB/8 items` 下完成 2/3/4/5 ms 扫描，持续上传和
mixed 共 `288/288` 条记录成功。4 ms 仅把 64 MiB/c4 持续吞吐从 2 ms 的 `116.11` 提高到
`121.59 MiB/s`（+4.7%），却使 mixed 4+4 从 `205.08` 降至 `186.51 MiB/s`（-9.1%），上传 P95 从
`504.4` 增至 `600.8 ms`。5 ms 也没有稳定收益；3 ms 虽有较好的客户端 P95，但 mixed 的 Primary durable queue P95
由 `84.9` 增至 `110.0 ms`。

所以 3.0 当前固定 `MINIKV_V3_GROUP_COMMIT_DELAY_US=2000`，代码默认值无需修改。更长窗口能提高部分负载的满批率，
但共享 SSD 上增加的主动等待/同步竞争抵消了收益。完整证据见
[性能基线报告 6.8.18](MINIDRIVER_PERFORMANCE_BASELINE_AND_BOTTLENECK_REPORT_2026-08-25.md#6818-rf2-group-commit-25-ms-固定窗口选择2026-08-28)，
复跑入口为 `tools/run_v3_p5_group_delay_benchmark.sh`。

### P6：小对象、控制面、副本与下载优化

- [ ] Benchmark 默认支持 Keep-Alive，并保留 `Connection: close` 对照模式；
- [ ] Route 支持批量申请，Chunk Commit/Session 更新合并为明确的 WriteBatch；
- [ ] 对 64 KiB 对象测 Session/Route/Body/Commit 各阶段 object/s；
- [ ] 在真实跨机网络测 Replica connection reuse、RTT、限速、丢包和半开连接；
- [ ] Primary 本地聚合与副本网络发送保持独立，副本节点按自身磁盘状态聚合；
- [ ] 下载继续使用 `sendfile`，验证 output watermark、Range、热/冷缓存与 checksum；
- [ ] 若 CPU copy 仍是已证实瓶颈，再单独评估 `readv/splice/io_uring`。

**验收：** 小对象固定成本下降，大对象/副本正确性不回归；慢副本和慢下载都能沿既有背压链传递而不造成无界缓存。

### P7：Docker Compose 与真实三主机验收

- [ ] 生成一个多阶段 `Dockerfile.minidriver`，Release 构建带版本和符号策略；
- [ ] 单机 Compose 启动 Gateway + 1/2/3 DataNode，所有对象数据使用显式 volume；
- [ ] healthcheck 区分“进程活着”“可接新写”“durability degraded”；
- [ ] 三主机每台使用独立 Compose/Tailscale advertise address，不共享 DataNode volume；
- [ ] 容器记录 cgroup `memory.current/memory.events/io.stat`，并设置合理 fd/进程限制；
- [ ] 一条命令完成 build/up/readiness/benchmark/down，默认 down 不删除 volume；
- [ ] 原生与容器执行同一正确性/性能矩阵，量化容器开销。

**验收：** 新机器按文档可复现版本、配置、数据目录和测试结果；容器重启后 durable 对象可读，清理命令不会误删用户数据。

### P8：有边界的动态调节与持续回归

- [ ] 静态矩阵稳定后，才加入每 volume 自适应 Group Commit；
- [x] 当前共享 SSD/RF=2 静态 delay 已扫描 2/3/4/5 ms，并固定 2 ms；这不是跨硬件通用值；
- [ ] 控制器仅调整 4～16 MiB batch、1～5 ms delay、4～16 items，不改变 durability contract；
- [ ] 输入使用 fdatasync EWMA、group wait、pending bytes、Dirty/Writeback、disk await 和前台 P99；
- [ ] checksum/identity 属于对象格式策略，不随负载动态切换；动态控制只处理资源预算与合批参数；
- [ ] 同时观察吞吐斜率与尾延迟：收益低于 5% 且 P95 增幅超过 20% 时禁止继续升档；
- [ ] 连续多个周期越界才调整，一次只移动一档，并设置 10～30 秒冷却防振荡；
- [ ] 磁盘已饱和时降低 admission，而不是无限扩大 batch/page cache；
- [ ] 将 correctness、buffered、durable、mixed 和 restart recovery 加入发布门禁；
- [ ] 保存每次回归的 git revision、config digest、硬件、原始结果与火焰图。

**验收：** 自适应策略在突发、低 QPS、持续写和 mixed 负载下不劣于静态安全默认，且所有参数始终停留在硬边界内。

## 8. 实施纪律与兼容策略

```text
每次只启用一个主要变量；
新路径必须有运行时开关和旧路径回退；
数据格式变化必须先双读，再切新写，最后才考虑迁移旧对象；
默认配置变化必须有完整 benchmark 与正确性证据；
HTTP 200 的含义必须和 durability mode 一致；
任何性能数字都必须注明 RF、sync、缓存、网络与对象大小。
```

禁止直接把当前所有 LevelDB `WriteOptions` 全局替换成 `sync=true`。Data extent、Physical Index、Gateway Session、日志和
未来 Metadata 的持久化边界不同，必须在各自事务顺序中明确处理。也禁止一开始同时引入 `pwritev + CRC32C + opaque
chunkId + Group Commit`，否则出现损坏或性能变化时无法定位责任变量。

## 9. 与 3.1 高可用的接口约束

| 3.0 数据面产物 | 3.1 如何复用 |
|---|---|
| `ChunkWriteDescriptor` | Metadata Route 生成描述符，但不处理 Body |
| `DurableReplicaResult` | Raft Commit 只记录已经满足策略的副本结果 |
| `generation/nodeEpoch` | Repair、旧节点与旧回调 fencing |
| `DurabilityCoordinator` | 每个 DataNode/volume 本地使用，不进入 Raft |
| `ReplicaTransport` | Repair 可增加新 Transport 实现，不改 ChunkStore |
| 性能门禁 | 每次接入 Raft/Repair 后重复，Body 吞吐与控制延迟分开报告 |

3.1 允许因 majority commit 和 Repair 增加可解释的延迟，但不得让文件 Body 经过 Gateway、Raft、Redis 或 Metadata
StateMachine，也不得绕过 3.0 的持久化 ACK 顺序。

## 10. 不应过早做的事情

```text
把 maxActiveUploads 盲目调大；
为“更快”取消校验或严格副本确认；
将文件 Body 放入 Redis、Raft 或 Gateway；
仅跑 loopback 就宣称跨机性能；
为了协议新潮立刻切 HTTP/2、QUIC、gRPC 或 io_uring；
在静态参数没有基线前加入复杂自适应；
在 Compose 还不可复现时先上 Kubernetes。
```

3.0 的工程顺序固定为：**基线 → 可观测 → per-Chunk durable → 聚合写 → Group Commit → 协议/身份解耦 →
小对象与副本优化 → Compose/真实主机 → 有界自适应。**
