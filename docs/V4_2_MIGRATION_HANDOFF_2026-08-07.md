# V4.2 Multi-Reactor 迁移与续作交接

> 日期：2026-08-07
> 当前分支：`v2`
> 当前 HEAD：`3d4333c docs: plan V4.2 multi-reactor datanode`
> 状态：V4.2 核心代码已实现并通过功能回归；性能矩阵未完成，必须迁移到资源充足的机器继续。

## 1. 为什么现在停止本机压测

当前机器已经不适合继续进行并发性能测试：

```text
根分区：/dev/vda2，40 GiB，使用率 100%，剩余约 21 MiB
内存：  约 3.6 GiB
Swap：  0
```

完整 CTest 首次执行时，Playwright/Chromium 因根分区写满发生 `Page crashed`。
将 `TMPDIR` 临时切换到 `/dev/shm` 后，46 项测试全部通过，证明该失败不是业务回归。

后续并发性能矩阵使用 `/dev/shm` 临时运行。在人为把 DataNode 写入准入上限提高到 8 后，
8 并发出现 `chunk length or SHA-256 verification failed`；随后双 I/O Loop 矩阵运行期间机器
发生明显资源压力，测试被中止。没有采集到完整的 OOM/kernel 证据，因此不能直接断言是
OOM，也不能把该现象归因于 Multi-Reactor。

结论：不要在本机继续性能调参，也不要基于当前不完整样本宣称 V4.2 提升或回退。

## 2. V4.2 已完成内容

设计与实施计划：

- `docs/superpowers/specs/2026-08-07-v4-multi-reactor-design.md`
- `docs/superpowers/plans/2026-08-07-v4-multi-reactor.md`

### 2.1 EventLoop 线程基础设施

已新增：

```text
include/network/EventLoopThread.hpp
src/network/EventLoopThread.cpp
include/network/EventLoopThreadPool.hpp
src/network/EventLoopThreadPool.cpp
```

实现内容：

- `EventLoopThread` 安全发布线程内 EventLoop，析构时 `quit + join`；
- `EventLoopThreadPool` 固定线程数、轮询分配、启动失败回滚；
- `threadNum=0` 时回退到 base EventLoop；
- `stop()` 幂等，避免线程或 EventLoop 超过所有者生命周期。

### 2.2 TcpServer Multi-Reactor

已修改：

```text
include/network/Acceptor.hpp
src/network/Acceptor.cpp
include/network/TcpServer.hpp
src/network/TcpServer.cpp
include/network/TcpConnection.hpp
src/network/TcpConnection.cpp
```

当前模型：

```text
base EventLoop
  -> accept4() 持续 accept 到 EAGAIN
  -> 独占 TcpServer::connections_ 注册表
  -> round-robin 选择 worker EventLoop

worker EventLoop
  -> 独占连接 Channel
  -> socket read/write
  -> input/output buffer
  -> HTTP parser 与连接业务状态
```

关闭连接采用两阶段投递：

```text
worker loop 收到 close
  -> base loop 删除 connections_ 记录
  -> worker loop 执行 connectDestroyed() 和 Channel remove
```

`TcpConnection` 新增不可变的 `ownerLoop()` 和连接局部 `std::any context`。

### 2.3 HTTP 状态线程亲和

已修改：

```text
include/http/HttpServer.hpp
include/http/DeferredResponse.hpp
src/http/DeferredResponse.cpp
```

原来的 `HttpServer::contexts_` 是一个进程级、无锁 map；开启多个 EventLoop 后会产生并发访问。
现在每条连接把自己的 `shared_ptr<HttpContext>` 存入 `TcpConnection` context，不再共享 parser map。

立即响应和 `DeferredResponse` 都根据 `TcpConnection::ownerLoop()` 回到原连接线程，禁止 base loop
直接修改 worker loop 的连接和 HTTP 状态。

### 2.4 DataNode 接入

`src/DataNode/datanode_main.cpp` 已完成：

- `MINIKV_V4_IO_THREADS`，默认 `2`，允许 `0..32`；
- `server.setThreadNum(ioThreads)`；
- 启动日志增加 `io_threads=N`；
- `ChunkUploadStream` 绑定 `upstream->ownerLoop()`；
- 每条上传流创建属于同一 owner loop 的 `HttpGatewayControlClient`；
- 注册和心跳仍由 base loop 上的控制客户端负责；
- `consume/finish/磁盘完成/副本完成/commit/响应完成` 增加 loop-affinity 断言；
- 异步边界继续使用 `weak_ptr`，不跨线程捕获裸 `this`。

## 3. 已完成验证

构建命令：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j2
```

构建结果：所有目标成功。

完整回归命令：

```bash
TMPDIR=/dev/shm ctest --test-dir build --output-on-failure
```

结果：

```text
46/46 passed
Total Test time: 9.06 sec
```

覆盖范围包括：

- EventLoopThread / EventLoopThreadPool；
- TcpConnection context；
- TcpServer / HttpServer Multi-Reactor；
- AsyncHttpClient timeout；
- HTTP 流式 Body；
- DataNode 删除、D1 磁盘流水线、资源配额；
- sendfile、FastDataStore；
- Gateway 元数据、缓存、上传 preflight；
- Redis 媒体任务、JPEG/RAW 派生图；
- Playwright 缩略图前端测试；
- benchmark 组件测试。

DataNode 还分别验证了：

```text
MINIKV_V4_IO_THREADS=2：通过
MINIKV_V4_IO_THREADS=0：通过
```

生命周期压力命令已经执行过，但终端输出在长重复过程中被截断。迁移后应重新执行并保留结果：

```bash
ctest --test-dir build \
  -R '^(event_loop_thread|event_loop_thread_pool|tcp_server_multi_reactor|http_server_multi_reactor)$' \
  --repeat until-fail:100 --output-on-failure
```

## 4. 当前性能样本及其限制

测试对象为 16 MiB、双 DataNode、双副本、本机 loopback、数据目录位于 tmpfs。
该环境不代表真实磁盘性能，只用于检查线程模型。

| 模式 | 并发 | 成功 | 上传 P50 | 下载 P50 | 备注 |
|---|---:|---:|---:|---:|---|
| I/O loops=0 | 1 | 3/3 | 142.531 ms | 49.654 ms | 样本抖动较大 |
| I/O loops=2 | 1 | 3/3 | 112.304 ms | 54.640 ms | 不能据 3 个样本宣称提升 |
| I/O loops=0 | 2 | 4/4 | 183.458 ms | 79.261 ms | 每轮聚合约 86.35/96.40 MiB/s |
| I/O loops=0 | 4 | 8/8 | 322.730 ms | 121.016 ms | 每轮聚合约 104.11/110.24 MiB/s |
| I/O loops=0 | 8 | 0/16 | 无效 | 无效 | 人为放宽准入后出现 Chunk 校验失败 |

双 I/O Loop 的 2/4/8 矩阵未完成。上表不能作为最终 V4.2 性能报告。

8 并发测试使用了：

```text
MINIKV_V4_MAX_ACTIVE_UPLOADS=8
MINIKV_V4_MAX_UPLOADS_PER_CLIENT=8
```

但 `DiskWriteExecutor` 仍是固定 `workerCount=2, blockCount=128`。只提高准入上限而不一起调整
磁盘 Worker、BlockPool 容量和全局 Chunk 窗口，会破坏原先的容量约束。迁移后必须先定位
校验失败的直接原因，不能继续盲目提高并发。

## 5. 当前 Git 状态

V4.2 的设计和计划已提交：

```text
724f1c4 docs: define V4.2 multi-reactor datanode
3d4333c docs: plan V4.2 multi-reactor datanode
```

V4.2 代码当前仍在工作区，没有提交。工作区还混有此前 V4.1、sendfile、前端并发上传等
已实现但未提交的修改，因此不要使用 `git reset --hard`，也不要只复制几个 V4.2 文件。

关键新增文件：

```text
include/network/EventLoopThread.hpp
include/network/EventLoopThreadPool.hpp
src/network/EventLoopThread.cpp
src/network/EventLoopThreadPool.cpp
test/test_event_loop_thread.cpp
test/test_event_loop_thread_pool.cpp
test/test_tcp_connection_context.cpp
test/test_tcp_server_multi_reactor.cpp
test/test_http_server_multi_reactor.cpp
```

关键修改文件：

```text
CMakeLists.txt
include/network/Acceptor.hpp
include/network/TcpConnection.hpp
include/network/TcpServer.hpp
include/http/HttpServer.hpp
include/http/DeferredResponse.hpp
src/network/Acceptor.cpp
src/network/TcpConnection.cpp
src/network/TcpServer.cpp
src/http/DeferredResponse.cpp
src/DataNode/datanode_main.cpp
test/test_v2_datanode_delete.sh
```

`HttpResponse`、CORS、sendfile 测试、`NodeResourceGovernor`、`www-v2` 等脏文件来自相邻阶段，
是当前可运行整体的一部分。提交前必须通过 `git diff` 审查，不要误删。

## 6. 推荐迁移方法

### 6.1 在当前机器创建交接提交

先创建独立分支：

```bash
git switch -c feat/v4-multireactor-handoff
git status --short
git diff --check
```

如果当前所有 tracked 修改都是需要保留的 V4.1/V4.2 工作：

```bash
git add -u
git add \
  include/DataNode/NodeResourceGovernor.hpp \
  include/network/EventLoopThread.hpp \
  include/network/EventLoopThreadPool.hpp \
  src/network/EventLoopThread.cpp \
  src/network/EventLoopThreadPool.cpp \
  test/test_event_loop_thread.cpp \
  test/test_event_loop_thread_pool.cpp \
  test/test_http_file_response.cpp \
  test/test_http_server_multi_reactor.cpp \
  test/test_node_resource_governor.cpp \
  test/test_tcp_connection_context.cpp \
  test/test_tcp_server_multi_reactor.cpp \
  docs/V4_1_MIXED_LOAD_GOVERNOR.md \
  docs/V4_2_MIGRATION_HANDOFF_2026-08-07.md

git diff --cached --stat
git diff --cached --check
git commit -m "feat: add V4 multi-reactor datanode"
git push -u origin feat/v4-multireactor-handoff
```

不要顺手提交 `build/`、`build-perf/`、`data/`、`perf.data*`、火焰图、个人 note、
`AGENTS.md` 或 `CLAUDE.md`，除非明确决定这些也属于仓库。

### 6.2 在目标机器拉取

```bash
git clone --branch feat/v4-multireactor-handoff --single-branch \
  git@github.com:1hongpuy/miniDriver.git miniDriver-v4
cd miniDriver-v4
```

如果仓库已经存在：

```bash
git fetch origin --prune
git switch --track origin/feat/v4-multireactor-handoff
```

若目标机器存在本地修改，先执行 `git status`，选择提交或 `git stash push -u`，不要强制覆盖。

## 7. 目标机器要求与初始化

推荐最低资源：

```text
CPU：       8 核
内存：      16 GiB
Swap：      4-8 GiB（防止测试工具和编译同时触发 OOM）
可用磁盘：  至少 100 GiB；完整矩阵建议使用独立 300-500 GiB 数据盘
```

依赖：

```bash
sudo apt update
sudo apt install -y \
  build-essential cmake pkg-config \
  libleveldb-dev libssl-dev libyaml-cpp-dev libspdlog-dev \
  libhiredis-dev libraw-dev redis-server \
  python3 python3-pip curl
```

若要运行前端测试：

```bash
python3 -m pip install --user playwright
python3 -m playwright install chromium
```

测试前确认：

```bash
df -h
free -h
swapon --show
ulimit -n
```

构建目录、benchmark 工作目录和 DataNode 数据目录应放在大容量数据盘，不要再使用根分区。

## 8. 迁移后的执行顺序

### Step 1：先恢复正确性基线

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

然后执行 100 轮生命周期测试。任何 hang、abort 或 fd 增长都应先修复，不能直接压测。

### Step 2：复现并定位 8 并发 Chunk 校验失败

需要同时采集：

```text
DataNode chunk_body_rejected / chunk_local_finish_failed 日志
DiskWriteExecutor queue bytes、BlockPool 可用块数
进程 RSS 和线程 CPU
dmesg 中的 OOM/kill 信息
每个 Chunk 的 expected size/hash、accepted bytes、written bytes
```

推荐监控命令：

```bash
vmstat 1
pidstat -rud -p <gateway_pid>,<node_a_pid>,<node_c_pid> 1
iostat -xz 1
dmesg -T | tail -n 100
```

先验证默认准入 `2`。之后每次只提高一个参数，并满足：

```text
全局活动 Chunk 数 * 单流最大排队字节
  < BlockPool 总容量 - 安全余量
```

在 `DiskWriteExecutor` 的 Worker 数和 BlockPool 容量变成可配置前，不应把 8 并发当成
正式支持能力。

### Step 3：完成严格的 0 Loop / 2 Loop 对比

每个并发档位必须使用新的 DataNode/Gateway 数据目录，避免内容去重污染写盘结果。
两组测试必须使用相同构建、磁盘、副本数、Chunk 内容、准入参数和运行轮数。

至少记录：

```text
上传：1/2/4/8 并发吞吐和 P50/P95/P99
下载：1/2/4/8 并发吞吐和 P50/P95/P99
混合：上传和下载同时运行的聚合吞吐
CPU：进程总 CPU、每个 I/O Loop 线程 CPU
EventLoop：loop lag P50/P95/P99
背压：pause 次数、pause 时间、outputBuffer 峰值
磁盘：队列峰值、等待时间、util、await
正确性：成功率、503 数量、400 数量、最终 SHA-256
```

当前 benchmark 尚不能完整导出 mixed load、per-thread CPU 和 EventLoop lag，需要先补指标，
不能用单一文件吞吐代替完整 V4.2 验收。

### Step 4：形成最终报告

目标文件：

```text
docs/V4_2_MULTI_REACTOR_PERFORMANCE_REPORT_2026-08-07.md
```

必须保留原始 CSV、命令、硬件和配置。若瓶颈仍是磁盘、SHA、短连接或同步副本，报告应如实
说明，不能把 Multi-Reactor 描述成吞吐必然翻倍。

## 9. V4 后续尚未完成的内容

V4.2 完成并不等于整个 V4 完成。后续仍包括：

1. **ReplicaConnectionPool**：DataNode 间从每 Chunk 短 HTTP 连接升级为健康检查、重连和复用的长连接池。
2. **SharedBodyBlock + writev**：减少接收、磁盘队列和副本转发之间的重复复制；正确处理 partial write。
3. **并行 Chunk 窗口**：客户端按节点容量调节窗口，不让多个文件和重试互相抢占。
4. **自适应背压与公平调度**：上传、下载、Repair、缩略图使用独立配额，避免大流量饿死控制请求。
5. **磁盘调度增强**：Worker/BlockPool 配置化，读写队列隔离，并根据真实磁盘决定是否采用 io_uring。
6. **完整混合负载基准**：并发上传、并发下载、同文件下载、上传下载混合和故障注入。

以下仍属于 V5，不要混入 V4.2：

```text
DataNode index WAL/snapshot/recovery
scrub/FreeList/持久 GC
持久 RepairTask
多 Gateway 一致性
用户 ACL 与审计
```

## 10. 给后续 AI 的第一条指令

```text
先阅读：
1. docs/V4_2_MIGRATION_HANDOFF_2026-08-07.md
2. docs/superpowers/specs/2026-08-07-v4-multi-reactor-design.md
3. docs/superpowers/plans/2026-08-07-v4-multi-reactor.md

不要重写已经完成的 Multi-Reactor；先在新机器完成全量回归和 8 并发失败根因定位，
然后补齐 io_threads=0 与 io_threads=2 的同条件性能矩阵。
```
