# V2 修复计划：性能重构前先修正基线与语义

> 状态：待逐项实现。本文依据本机压测和代码审查整理，目标是先让 V2 的正确性、
> 可测性和单机性能基线可信，再进入 V4 的多 Reactor 等性能重构。
>
> 性能原始数据见 [V2_FINAL_PERFORMANCE_ASSESSMENT_2026-07-29.md](/home/ubuntu/miniKV/v1.0/miniKVCine/docs/V2_FINAL_PERFORMANCE_ASSESSMENT_2026-07-29.md)。

## 1. 为什么不能直接做多 EventLoop

当前存在两类优先级更高的问题：

1. 构建实际是 Debug，吞吐和火焰图不能代表 Release 性能。
2. 写入并发的“路由许可”和 DataNode 的真实接纳能力不一致，压力下会出现有的
   Chunk 没有可靠写入、会话却继续推进的风险。

如果先上多 EventLoop，问题会被并发交错掩盖，定位成本会急剧升高。正确顺序是：

```text
可信 Release 基线
  -> 写入许可和幂等语义正确
  -> 可观测指标与回归测试
  -> 消除明显的读路径和复制路径浪费
  -> 有界磁盘执行器
  -> 多 EventLoop / 连接池 / 自适应窗口
```

---

## 2. Fix 0：修正构建类型，并移除热路径调试输出

### 原来的问题

[CMakeLists.txt](/home/ubuntu/miniKV/v1.0/miniKVCine/CMakeLists.txt:5) 当前无条件设置：

```cmake
set(CMAKE_BUILD_TYPE "Debug")
```

这会覆盖命令行中的 `-DCMAKE_BUILD_TYPE=RelWithDebInfo`。因此即便创建了
`build-perf`，实际编译参数仍只有 `-g`，没有 `-O2`，也没有 `-DNDEBUG`。

另外，HTTP 请求行和每次发送都在热路径写标准输出。例如：

- [HttpContext.hpp](/home/ubuntu/miniKV/v1.0/miniKVCine/include/http/HttpContext.hpp:176)
- [HttpServer.hpp](/home/ubuntu/miniKV/v1.0/miniKVCine/include/http/HttpServer.hpp:140)
- `datanode_main.cpp` 中每个 Chunk 的暂停、恢复、完成日志。

这些输出会竞争 `stdout` 锁、触发终端或文件写入，并污染火焰图。

### 为什么会导致错误判断

例如同样上传 64 MiB：

```text
Debug：函数未内联、边界检查更多、字符串和哈希调用开销放大。
Release：优化器可内联小函数、消除部分临时对象。
```

因此不能把当前 MiB/s 当成 V2 的最终性能，也不能据此决定是否立刻多线程化。

### 已落地的第一版

本轮已完成下列代码改动：

| 位置 | 改动 | 解决的问题 |
|---|---|---|
| [GatewayState.hpp](/home/ubuntu/miniKV/v1.0/miniKVCine/include/gateway/GatewayState.hpp) | 新增 `WriteLease`、`RoutePlanStatus`、按节点聚合的 reservation 计数 | 将“已经签发、尚未完成”的写入从心跳观测中独立出来 |
| [GategayState.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/gateway/GategayState.cpp) | 路由签发原子预留，重复请求复用租约，commit/超时/节点离线释放租约 | 避免 8 秒心跳间隔内反复超额签发 |
| [gateway_main.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/gateway/gateway_main.cpp) | 容量不足返回 `503 Service Unavailable` 和 `Retry-After: 1` | 客户端能区分“请求写错”与“稍后重试” |
| [WriteAdmission.hpp](/home/ubuntu/miniKV/v1.0/miniKVCine/include/DataNode/WriteAdmission.hpp) | 每个 DataNode 的本地原子写槽 | Gateway 遗漏、租约超时或直接访问 DataNode 时，节点仍不会超过自身能力 |
| [datanode_main.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/datanode_main.cpp) | 写入开始前 `tryAcquire()`，结束、异常和析构时归还 | 将 `maxConcurrentWrites=2` 变为真正的本地硬限制 |

当前固定策略是：每个 DataNode 最多 `2` 个正在落盘的 Chunk。这个数字是当前
2C/2G 节点的保守起点，不是普适最优值。下一阶段应让它来自节点 YAML，并通过压测
确定不同机器的值；接口已经不依赖具体数字。

验证代码：

```text
test/test_gateway_write_lease.cpp
  两节点 x 两槽：重复 route 不重复占槽；第三路得到 kNoCapacity；commit 后槽位释放。

test/test_write_admission.cpp
  本地准入器：两次 acquire 成功，第三次失败；release 后可再次 acquire。
```

### 当前保留限制

HTTP/1.1 的 `Content-Length` Body 已经进入流式解析后，现有 `HttpServer` 才会调用
`ChunkUploadStream`。因此 DataNode 满载时，为了保持该 HTTP 请求的解析状态正确，
当前实现会继续丢弃并消费该请求剩余 Body，最后回复 `503`；它不会执行磁盘写入、
SHA-256 或副本转发。真正的“收到 headers 就拒绝、完全不接收 Body”需要给
`HttpServer` 增加 header 阶段的拒绝结果，这属于下一次协议层小改，不和本轮的
Gateway 租约机制混在一起。

### 修复方案

1. 不强制覆盖用户指定的构建类型：

```cmake
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()
```

2. 保留 `Debug`、`RelWithDebInfo`、`Release` 三种显式构建目录。
3. 日志改为可配置等级：`ERROR/WARN/INFO/DEBUG/TRACE`。
4. 请求级日志只在 `DEBUG`；Body 分段、背压反复触发只在 `TRACE`，并支持采样。

### 验收

```bash
cmake -S . -B build-perf -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build-perf -j"$(nproc)"
grep -R -- '-O2\|-DNDEBUG' build-perf/CMakeFiles/*/flags.make
```

压测报告必须同时记录：Git commit、编译类型、CPU、磁盘、网络、命令行参数。

---

## 3. Fix 1：Placement 的写入许可必须和 DataNode 接纳能力一致

### 原来的问题

Gateway 在 [GategayState.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/gateway/GategayState.cpp:295) 根据心跳中的 `activeUploads` 做硬过滤：

```cpp
runtime.activeUploads >= record.maxConcurrentWrites
```

但 `activeUploads`：

- 由 DataNode 约每 8 秒才通过心跳上报；
- 是滞后的观测值，不是原子占位；
- DataNode 本身没有在接收 HTTP Body 前执行同样的接纳检查。

DataNode 在 [datanode_main.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/datanode_main.cpp:252) 才增加写入计数，已经晚于 Gateway 发路由。

### 错误如何发生

假设 `maxConcurrentWrites = 2`，两个节点都刚上报 `activeUploads = 0`：

```text
t0  Gateway 同时收到 8 个 routes 请求
t1  每次都读到旧心跳：A=0, C=0
t2  Gateway 为很多 Chunk 都签发了 A/C 路由
t3  A/C 才开始收到 HTTP 请求并递增 activeWrites
t4  下一次心跳之前，Gateway 仍不知道节点已经拥塞
```

结果不是“负载均衡”，而是过量签发。此前 64 MiB、8 文件并发出现
`16 个请求只成功 8 个`，就是该时间窗口的证据。

### 修复方案

Gateway 在签发路由时创建短期 **WriteLease**：

```cpp
struct WriteLease {
    std::string leaseId;
    std::string sessionId;
    uint32_t chunkIndex;
    std::string nodeId;
    uint64_t bytes;
    int64_t expiresAtMs;
};
```

选择节点后，在同一把锁/同一状态事务里增加本地 reservation。当前 V2 的硬上限
只信任 Gateway 自己刚创建的 reservation；心跳中的 `activeUploads` 仅参与评分，
不能作为硬拒绝条件，因为它必然滞后：

```text
hard admission: gateway.reservedWrites < maxConcurrentWrites
placement score: heartbeat.activeUploads + gateway.reservedWrites
space check:     heartbeat.freeBytes - gateway.reservedBytes
```

只有 reservation 未达到上限的节点才可被再次分配。以下事件释放 reservation：

- Gateway 收到成功的 Chunk commit；
- 租约超时；
- 节点被标记 Offline。

当前没有客户端取消 Session 或 DataNode 失败回调接口，因此失败后的 reservation
由 120 秒租约回收；这是下一步需要补齐的控制面 API，不应假装已经即时释放。

同一 `(sessionId, chunkIndex, chunkHash, chunkSize)` 的重复 routes 请求必须返回原
lease，而不是重复占一个名额。

DataNode 还需要 **本地 admission permit**：在接受 HTTP Body 前原子申请一个写槽。
满载时返回明确的 `503 Service Unavailable` 与 `Retry-After`，而不是读了一部分 Body
后才以 `400` 失败。

### 验收

- 两节点、每节点 `maxConcurrentWrites=2` 时，同时发 8 个文件：请求可以排队或被
  明确重试，但不能随机丢失或超额写入。
- 路由租约过期后名额会归还。
- 重复请求同一路由不会增加 `reservedWrites`。
- Gateway 的 `/admin/nodes` 展示 `heartbeatActiveWrites` 和 `reservedWrites`，避免
  调试时混淆两者。

---

## 4. Fix 2：Chunk commit 必须幂等

### 原来的问题

[GategayState.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/gateway/GategayState.cpp:455) 对已完成的同一索引直接返回失败：

```cpp
if (session.completedChunks.count(index) != 0) {
    return false;
}
```

网络超时的正常场景是：DataNode 已把 commit 发到 Gateway，Gateway 已落盘，但 HTTP
响应在途中丢失。DataNode 重试 commit 后，Gateway 却把“相同的成功”当成失败。

### 修复规则

```text
同一个 session/index/hash/size：200，返回既有结果，并合并新确认的副本节点。
同一个 session/index，但 hash 或 size 不同：409 Conflict。
Session 不存在或已取消：404/409，按状态明确返回。
```

更新 `ChunkRoute`、`SessionState`、租约释放应尽可能在同一个 LevelDB `WriteBatch`
中完成，防止只更新其中一项。

### 验收

对同一个 commit 连续调用两次，第二次也成功；会话已完成 Chunk 数不增加两次，路由
副本列表不出现重复节点。

---

## 5. Fix 3：Manifest 构建必须基于快照

### 原来的问题

[gateway_main.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/gateway/gateway_main.cpp:162) 构建每个
Chunk 时都再次调用 `state.getRoute()`，同时还反复复制 `state.nodes()`。

对于一个 1 GiB 文件（256 个 4 MiB Chunk），一次 manifest 可能触发数百次锁竞争和
节点表复制。并发下载时，控制面尾延迟会被无谓放大。

### 修复方案

GatewayState 增加只读快照接口：

```cpp
struct ManifestSnapshot {
    FileMeta file;
    std::vector<ChunkRoute> routes;
    std::unordered_map<std::string, NodeRecord> nodes;
};

bool buildManifestSnapshot(const std::string& fileHash,
                           ManifestSnapshot* snapshot);
```

在 GatewayState 内部持锁读取所需数据一次，释放锁后由 HTTP Handler 组装 JSON。
不要在锁内做字符串拼接、JSON 序列化或网络发送。

### 验收

- 256 Chunk manifest 只复制一次节点表。
- 上传 commit 与 manifest 并发时，不产生部分新、部分旧的不可解释路由。
- 增加 `manifest_build_ms` 指标。

---

## 6. Fix 4：下载走 extent + sendfile，而不是完整 Chunk 读入字符串

### 原来的问题

[FastDataStore.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/FastDataStore.cpp:155) 的
`get()` 会：

```text
查询 hash -> 得到 offset/length
  -> pread 整块到 std::string
  -> 重新计算 SHA-256
  -> HTTP 输出缓冲
  -> socket send
```

一个 4 MiB Chunk 因此至少经过磁盘到用户态、用户态到输出缓冲、用户态到内核的多次
复制。下载火焰图中 `pread/FastDataStore::get` 是明显热点。

### 修复方案

`FastDataStore` 提供只查索引的接口：

```cpp
struct FileRegion {
    int fd;
    off_t offset;
    size_t length;
};

bool getRegion(const std::string& hash, FileRegion* out) const;
```

HTTP 先发送 headers，再让 `TcpConnection` 对该 region 调用 `sendfile`：

```text
path      = disk0.data
offset    = extent.offset
remaining = extent.length
```

现有 [TcpConnection.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/network/TcpConnection.cpp:347)
已具备 sendfile 状态，但 `startSendFile()` 把 offset 固定为 0；需要改为接受 region
offset 和 length。

### 哈希校验边界

下载时服务器不应为每一次读取重新计算整个 Chunk SHA-256。正确边界是：

- 上传时 DataNode 必须流式计算并校验 SHA-256；
- Gateway manifest 向客户端提供每个 Chunk 的预期 hash；
- 客户端下载后校验 hash，失败则换副本；
- V5 的后台 scrub 定期从磁盘抽样/全量校验，并负责修复损坏副本。

这不是取消数据完整性，而是把每次下载的重复 CPU 工作移到端侧校验和后台巡检。

### 验收

- `curl` 下载 Chunk 的长度与 sha256 正确。
- 下载路径中没有完整 `std::string` Chunk 分配。
- `sendfile` 的 offset 对非零 extent 正确，不能永远读 `disk0.data` 开头。

---

## 7. Fix 5：降低已知的应用层复制和队列开销

### 原来的问题

[TcpConnection.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/network/TcpConnection.cpp:213) 的
`send(const char*, size_t)` 先复制成 `std::string`；随后跨线程 lambda 再按值捕获；不能
立即发送时还会进入 output buffer。旧火焰图中 `__memmove` 约占 6.49%。

[ReplicaUploadPipe.hpp](/home/ubuntu/miniKV/v1.0/miniKVCine/include/DataNode/ReplicaUploadPipe.hpp:85)
使用 `std::vector<std::string> pendingBlocks_`；
[ReplicaUploadPipe.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/ReplicaUploadPipe.cpp:197)
从头 erase，会搬移后续元素。

### 修复方案

第一步只做低风险改动：

```cpp
void TcpConnection::send(std::string data);
```

调用方构造一次，跨线程时 move 进 EventLoop lambda。`pendingBlocks_` 改为
`std::deque<std::string>`，消费使用 `pop_front()`。

这会减少临时对象和队列移动，但不会把上传变成“真正零拷贝”。上传数据仍必须经过用户
态，因为同一段 bytes 必须完成：

```text
SHA256_Update + pwrite + 向副本 socket 转发
```

后续 V4 才评估共享不可变块、`iovec/writev` 和引用计数缓冲。必须先有 buffer
所有权、背压和生命周期测试，不能仅用 `string_view`，否则异步发送时引用可能悬空。

### 验收

- 慢副本下不发生 use-after-free。
- `pendingBlocks_` 不再线性搬移。
- 火焰图中 `__memmove` 占比下降，且 Chunk hash、长度、复制结果均正确。

---

## 8. Fix 6：补齐可观测性和自动化回归

### 原来的问题

目前能看到最终吞吐，却无法在运行中回答：

- 是否发生背压、发生多少次、最长多久？
- 输出缓冲峰值是多少？
- EventLoop 是否被磁盘写阻塞？
- 路由失败是容量、写槽满、租约过期，还是连接失败？

另外已有独立测试二进制，但 CMake 没有 `add_test()`，`ctest` 看不到它们。

### 修复方案

新增低成本计数器和直方图：

```text
gateway.routes_reserved_total
gateway.routes_rejected_total{reason}
gateway.commit_idempotent_total
datanode.write_admission_rejected_total
datanode.replica_pause_total
datanode.replica_pause_duration_ms
datanode.output_buffer_peak_bytes
datanode.chunk_upload_latency_ms (P50/P95/P99)
eventloop.task_lag_ms
```

将现有测试添加到 CTest，并新增：

1. 路由租约重复申请不重复预占。
2. DataNode 写槽满时返回 503。
3. commit 重试幂等。
4. 非零 offset 的 sendfile region。
5. manifest snapshot 在并发 commit 下结构完整。

### 验收

压测输出能直接给出吞吐、P50/P95/P99、pause 次数、输出队列峰值、路由拒绝原因；
`ctest --test-dir build-perf --output-on-failure` 能运行测试。

---

## 9. Fix 7：先做有界磁盘执行器，再做多 EventLoop

### 原来的问题

`ChunkUploadStream::consume()` 在 DataNode I/O EventLoop 中直接执行
`SHA256_Update + pwrite`。慢盘或写放大会让整个 loop 无法及时处理其他连接、心跳和
`EPOLLOUT`。

### 修复方案

不要把任意 `ThreadPool` 直接接到每个数据块上。需要一个**有界、按流保序**的磁盘
执行器：

```text
I/O EventLoop
  -> 每流最多 N 个已提交磁盘任务
  -> 全局字节队列有上限
  -> DiskExecutor 执行 pwrite / hash 分段
  -> queueInLoop 回传完成结果
  -> 再决定 resumeRead 或 abort
```

队列满时暂停上游读取，不能无限提交 64 KiB 任务。每个 Chunk 内的 offset 和 hash
更新必须严格有序。

### 验收

对一个节点施加慢盘压力时：其他连接仍能收发心跳和小请求；内存不随上传时长无限增长；
同一 Chunk 的最终 hash 正确。

---

## 10. V4：多 EventLoop 及后续性能重构

只有前述修复通过后，才进入这些增强：

```text
单 I/O loop             -> acceptor + N 个 I/O loops
短副本连接              -> 每目标节点连接池
固定水位线              -> 基于 RTT/队列/磁盘的自适应窗口
内存 Chunk 下载         -> extent/sendfile
单条 commit             -> 合法范围内的小批量 WriteBatch
简单临时缓冲            -> 共享块 + iovec/writev（先测试生命周期）
```

多 Reactor 的目的不是“线程越多越快”，而是让连接固定归属某个 loop，以多个 CPU 核
处理协议事件；磁盘阻塞由有界执行器隔离；慢副本由背压只影响关联流。

---

## 11. 建议提交顺序

每一步单独提交、单独测量，避免把多个变量混在一起：

1. `fix(build): respect requested build type and gate hot-path logs`
2. `test(v2): register local regression tests with ctest`
3. `fix(gateway): reserve write leases during placement`
4. `fix(datanode): enforce local write admission before body`
5. `fix(gateway): make chunk commit idempotent`
6. `refactor(gateway): build manifest from a consistent snapshot`
7. `feat(datanode): serve extents through sendfile regions`
8. `refactor(network): remove avoidable send and replica queue copies`
9. `feat(datanode): add bounded ordered disk executor`
10. `feat(network): introduce multi-reactor data plane`

每一个提交都至少运行构建、相关单元测试和一条本地上传/下载回归；性能提交还要附同一
机器、同一编译类型、同一数据规模的前后对比。
