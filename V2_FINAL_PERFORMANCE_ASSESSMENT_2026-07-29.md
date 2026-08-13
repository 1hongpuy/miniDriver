# MiniKVCine V2 性能测试结论与优化决策

> 日期：2026-07-29
> 范围：当前 Gateway + 两个 loopback DataNode 的 V2 数据面。
> 结论口径：本报告只把实际采集到的数据称为“结果”；无法测量的项目明确标为缺口。

## 1. 本次结论

V2 的流式上传、链式副本、直传、下载和断点会话链路已经能在本机稳定跑通。单流吞吐合理，
但并发达到 4 后聚合吞吐明显趋于饱和，8 并发进一步显著拉高尾延迟。当前最优的临时运行区间是：

```text
文件并发：1--2（推荐）
Chunk 窗口：1（V2 当前能力）
DataNode 最大并发写：必须同时由 Gateway 和 DataNode 共同限制
```

在实施 V4 性能重构前，必须先处理一个 V2 正确性问题：Gateway 将 8 秒心跳里的
`activeUploads` 当作精确硬闸门，可能在真实写入已经结束后仍拒绝所有新路由。

## 2. 验证范围与产物

构建和七个独立测试均通过：

```text
test_async_http_timeout
test_fast_data_store_read
test_http_stream_context
test_benchmark_types
test_benchmark_input
test_benchmark_http
test_benchmark_cli
```

项目当前没有 CMake `add_test()` 注册，所以 `ctest` 显示 `No tests were found`。这不代表测试
通过，独立二进制才是本次实际执行的验证入口。

新增本机矩阵产物：

```text
/tmp/minikv-v2-final-matrix-20260729-203849
/tmp/minikv-v2-control-plane-final-20260729-205655
/tmp/minikv-v2-mixed-20260729-205748
```

旧的 64 MiB、1 GiB 和火焰图基线见：

- [V2_LOCAL_PERFORMANCE_REPORT_2026-07-29.md](V2_LOCAL_PERFORMANCE_REPORT_2026-07-29.md)
- [V2_COMPLETE_PERFORMANCE_TEST_AND_ANALYSIS.md](V2_COMPLETE_PERFORMANCE_TEST_AND_ANALYSIS.md)

所有产物都在 `/tmp`，不进入 Git，也未清理既有压测或 profile 文件。

## 3. 数据面结果

### 3.1 已有较大文件基线

| 负载 | 上传吞吐 | 下载吞吐 | 端到端聚合中位数 |
|---|---:|---:|---:|
| 64 MiB，1 文件 | 277.6 MiB/s | 177.3 MiB/s | 81.08 MiB/s |
| 64 MiB，2 文件 | 202.3 MiB/s/文件 | 115.6 MiB/s/文件 | 117.38 MiB/s |
| 64 MiB，4 文件 | 104.9 MiB/s/文件 | 64.8 MiB/s/文件 | 132.20 MiB/s |
| 1 GiB，1 文件 | 153.6 MiB/s | 108.6 MiB/s | 51.71 MiB/s |

64 MiB、4 文件的三轮聚合结果为 `138.63 / 132.20 / 88.82 MiB/s`，已经存在明显波动；
不能把最高值写成稳定吞吐。

### 3.2 本轮 16 MiB 文件并发矩阵

每个文件有 4 个 4 MiB Chunk，每组 3 轮。下载和上传分位数是“每文件”样本，不是单 Chunk
分位数，因为当前 benchmark 尚未记录 Chunk 的分阶段时间线。

| 文件并发 | 成功数 | 上传 P50 / P95 / P99 | 下载 P50 / P95 / P99 | 每轮聚合端到端吞吐 |
|---:|---:|---:|---:|---:|
| 1 | 3 / 3 | 60.55 / 60.55 / 60.55 ms | 92.76 / 92.76 / 92.76 ms | 74.48, 78.85, 72.89 MiB/s |
| 2 | 6 / 6 | 79.35 / 93.48 / 93.48 ms | 135.83 / 138.77 / 138.77 ms | 110.45, 114.09, 108.35 MiB/s |
| 4 | 12 / 12 | 173.92 / 242.66 / 242.66 ms | 207.44 / 221.45 / 221.45 ms | 138.10, 136.38, 120.37 MiB/s |
| 8 | 24 / 24 | 371.52 / 524.78 / 528.30 ms | 226.06 / 323.81 / 335.14 ms | 140.86, 136.17, 141.95 MiB/s |

解释：从 1 到 2 文件并发，聚合吞吐约提升 45%。从 2 到 4 文件并发，聚合吞吐只有约 20%
增益，同时上传 P95 已增加到 242.66 ms。8 并发的聚合吞吐几乎没有超过 4 并发，但上传 P99
增加到 528.30 ms。因此 8 并发不是当前 V2 的有效性能模式。

### 3.3 读写混合样本

运行两个持续读取的 DataNode Chunk 下载循环，同时运行两个 16 MiB 上传流：

```text
上传端到端聚合吞吐：52.77 MiB/s
下载采样数：32
下载平均瞬时速度：121,565,079 B/s，约 115.9 MiB/s
```

对比纯上传 2 并发约 `108--114 MiB/s`，混合样本中上传聚合吞吐下降约一半。这是一个方向性
证据，不是长期稳定性结论：读取对象位于页缓存，且只运行一轮。

## 4. 控制面 QPS

所有请求为 loopback 小 JSON 请求，使用短 HTTP 连接。QPS 包含客户端启动 curl 进程的开销，
所以它是保守端到端数值，不是 Gateway 内部裸处理能力。

| API | 并发 | 总请求 | 成功 | QPS | P50 | P95 | P99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 创建 Session | 10 | 100 | 100 | 368.61 | 0.491 ms | 1.492 ms | 2.741 ms |
| 创建 Session | 50 | 200 | 200 | 381.32 | 0.521 ms | 3.587 ms | 6.715 ms |
| 分配 routes | 10 | 100 | 100 | 368.25 | 0.582 ms | 5.106 ms | 7.139 ms |
| 分配 routes | 50 | 200 | 200 | 373.11 | 0.571 ms | 3.056 ms | 5.153 ms |
| 查询 nodes | 10 | 100 | 100 | 397.74 | 0.441 ms | 1.652 ms | 4.107 ms |
| 查询 nodes | 50 | 200 | 200 | 423.11 | 0.439 ms | 1.223 ms | 2.451 ms |

这只能说明当前 Gateway 在小规模本机压力下尚未饱和。`commit` 与 `manifest` 的独立 QPS 没有
完成：现有 benchmark 不输出可复用 fileHash，且没有可控 mock DataNode。它们需要专门的测试
fixture，不能借用业务数据或手写不完整 JSON 伪造结果。

## 5. 已确认问题

### P0：Placement 使用陈旧 activeUploads，可能拒绝所有路由

64 MiB、8 文件并发的两次历史复现均在第一轮完成后出现下一轮全失败：

```text
64 MiB x 8 文件 x 2 轮：总 16 个，仅 8 成功
64 MiB x 8 文件 x 3 轮：总 24 个，仅 8 成功
失败阶段：POST /routes 返回 HTTP 400
```

本轮 16 MiB、8 并发能全部成功，证明该问题与心跳时序有关，而不是固定的并发上限。

根因链条：

```text
DataNode 在 Chunk 开始时递增 activeWrites
  -> 每 8 秒通过 heartbeat 上报一次
  -> Gateway 用 activeUploads >= maxConcurrentWrites 硬过滤候选节点
  -> 写完成后，Gateway 仍可能持有“忙”的旧心跳值最多 8 秒
  -> 两节点都被过滤，routes 返回 400
```

相关代码：

- [datanode_main.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/datanode_main.cpp:399)
- [datanode_main.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/datanode_main.cpp:492)
- [GategayState.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/gateway/GategayState.cpp:299)

**V2 修复方向：** Gateway 在成功签发 route lease 时预留写槽，在 commit、失败或 lease 到期时释放；
DataNode 自己必须实行同一上限并拒绝超额写流。心跳仅作为评分信号和异常兜底，不能充当精确
并发计数。此项优先于任何性能优化。

### P1：单个 I/O EventLoop 承担 hash、pwrite、复制和 HTTP 状态转换

`ChunkUploadStream::consume()` 在 EventLoop 回调中同步调用 `WriteSession::append()`；后者完成
SHA-256 更新与 `pwrite`。慢磁盘、CPU hash 或慢副本都会延迟该 loop 的其他连接。

- [datanode_main.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/datanode_main.cpp:174)
- [FastDataStore.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/FastDataStore.cpp:43)

这是当前并发超过 2 后尾延迟上升的主要结构性解释。它不是 epoll 性能差，而是回调做了阻塞
或 CPU 密集工作。

### P2：下载路径完整读取 Chunk 并重新 hash

`FastDataStore::get()` 将整个 4 MiB Chunk 读入 `std::string`，随后在服务端校验 SHA-256。
读写混合时，它与写路径争用磁盘、页缓存、CPU 和用户态内存。

- [FastDataStore.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/FastDataStore.cpp:155)

### P3：流式复制仍存在可见的用户态复制

已有火焰图样本显示 `__memmove_avx_unaligned_erms` 为约 `6.49%`。调用链包含：

```text
Http body
  -> ReplicaUploadPipe::push
  -> AsyncHttpRequest::writeInLoop
  -> TcpConnection::send(const char*, size_t)
  -> std::string / outputBuffer
```

- [ReplicaUploadPipe.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/ReplicaUploadPipe.cpp:87)
- [AsyncHttpClient.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/http/AsyncHttpClient.cpp:131)
- [TcpConnection.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/network/TcpConnection.cpp:213)

流式传输已经避免“完整 4 MiB Chunk 在每层复制”的无界内存问题，但它不等于零拷贝。

### P4：背压机制存在，但目前不可量化验证

高低水位、`pauseRead()` / `resumeRead()` 和 `ReplicaUploadPipe` 的 pause/resume 路径已存在。
但没有 pause 计数、持续时间、输出队列峰值或 EventLoop lag 指标。当前机器也没有无密码的
网络管理权限，不能在不影响系统的前提下使用 `tc netem` 注入慢副本。

因此只能确认“代码路径存在”，不能声称已证明内存上界或背压有效性。

### P5：测试基础设施不足

1. CMake 未注册 `add_test()`，`ctest` 无法运行测试。
2. benchmark 只输出文件级 upload/download，不输出每 Chunk 的 route、主写、副本 ACK、commit 时间。
3. benchmark 不输出已提交 fileHash，导致 manifest 控制面测试不能复用标准 fixture。
4. perf 被主机策略禁止：`kernel.perf_event_paranoid=4`。现有火焰图可供定位，但不能代表本轮矩阵。
5. 当前 `/tmp` 仅剩约 3.7 GiB，未运行 4 GiB 长文件与 30 分钟以上长稳测试。

## 6. 优化顺序

### V2 必须完成

1. 修复 Placement lease 槽位预留/释放，并在 DataNode 实施真实写入 admission control。
2. 为 Gateway/DataNode 加低基数指标：请求耗时、写入数、队列高水位、pause 次数/时长、
   EventLoop lag；不要把 sessionId 或 chunkHash 做 Prometheus label。
3. benchmark 输出 Chunk 时间线与 fileHash；将独立测试注册到 CTest。
4. 在 Node C 的 8C16G 环境重复本机矩阵，并单独执行 Tailscale 端到端矩阵。
5. 在隔离机器授予 perf 权限后重采集火焰图；不要为采样而降低生产机安全策略。

### V4 性能重构

1. 多 I/O EventLoop，连接固定归属，避免一个慢连接影响全节点。
2. 有界磁盘 worker pool：把 `pwrite`、`pread` 和 hash 从 EventLoop 移走。
3. Chunk 并发窗口和每节点公平配额；V2 仍保持 window=1。
4. 用明确的块所有权、`iovec` / `writev` 减少转发路径复制。
5. 下载使用 extent 的流式 `pread` 或 `sendfile`。已落盘健康副本由客户端按 manifest hash
   校验；后台 scrub 处理长期校验，不要求每次下载由服务端完整重 hash。
6. DataNode 间连接池和批量 metadata commit。

### V5 可靠性，不应混入本次性能修复

```text
WAL + snapshot + recovery
RepairTask / GC / scrub / FreeList / hole punching
多 Gateway 元数据一致性
多用户、权限、审计
```

这些决定恢复与可靠性语义，不是解决当前 2--8 并发吞吐曲线的第一手段。

## 7. 当前 V2 的可用边界

V2 可以作为学习和演示用的分布式对象存储数据面：流式切片、直传、链式副本、下载、续传和
基础背压路径均已存在。它还不能被表述为高并发或高可靠对象存储。

在 P0 修复、指标补齐和 Node C 重测前，建议将产品默认并发限制为 2 个文件、每文件 1 个
Chunk 在途，并将 8 并发仅作为压力测试，不作为承诺能力。
