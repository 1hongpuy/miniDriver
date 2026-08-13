# V4.3 阶段 0：下载与混合负载测试报告（2026-08-11）

## 结论

阶段 0 已补齐 benchmark 的 `download-only` 与真正的 `mixed` 执行路径，并完成真实
`/data` 磁盘上的双 DataNode 测试。下载专用在文件并发 `1/2/4/8` 下均成功；在
双节点、双副本、每节点两上传槽的 mixed 负载中，并发 2、4 全部成功，并发 8 的
失败全部是预期的 Gateway `503 + Retry-After` 写入准入拒绝。

本轮没有观察到下载 SHA-256 不匹配、Chunk 读取失败、异常 `400` 或进程悬挂。它验证了
下载槽配置在此压测口径下可用，也证明上传与下载同时运行时，当前写入准入仍按预期生效。
它**不**证明下载槽 8 是跨网络或长期生产负载下的最终上限。

## 本轮 benchmark 改动

- `--mode download`：先在计时区间外上传并 commit 独立夹具对象；随后只计 manifest、
  Chunk 下载和完整文件 SHA-256 校验时间。夹具准备耗时不进入下载指标。
- `--mode mixed`：每轮一半工作者下载预置且已 commit 的对象，另一半工作者同时进行上传；
  因而测量区间确实存在上传与下载并发。该模式要求 `--concurrency >= 2`。
- CSV 增加 `operation` 字段，区分 `upload` 与 `download`，避免把 download-only 的夹具
  上传时间或 mixed 的异类操作混入错误的百分位。
- 每个 benchmark 进程的对象名加入进程级唯一后缀。首次 smoke 曾发现不同 benchmark
  进程在同一远端目录复用确定性对象名，导致 commit `409`；此工具缺陷已修复并重新 smoke。

## 测试环境与口径

- 代码基线：`19f7b2e` 加上本轮 benchmark 未提交改动。
- 构建：`RelWithDebInfo`，GCC 11.4，CMake 3.22。
- 主机：8 vCPU、约 2.8 GiB 内存、2 GiB Swap；`/data` ext4，约 491 GiB。
- 部署：Gateway 1 个、DataNode 2 个、同机 loopback、链式双副本。
- DataNode：`MINIKV_V4_IO_THREADS=2`；每节点上传槽 2、下载槽 8；每客户端上传槽 2、下载槽 4。
- 文件：16 MiB，4 个 4 MiB Chunk；每档 3 轮。
- 成功条件：下载工作者对重组文件完成完整 SHA-256 校验；上传工作者全部 Chunk 双副本成功并 Gateway commit。

集群状态、日志和原始 CSV：

```text
/data/minikv-v2/v4-stage0-20260811/matrix-cluster
/data/minikv-v2/v4-stage0-20260811/matrix-results
```

执行形式：

```bash
MINIKV_V4_IO_THREADS=2 tools/start_v2_local_benchmark_cluster.sh

./build/bin/minikv_v2_bench local \
  --gateway 127.0.0.1:18521 \
  --work-dir /data/minikv-v2/v4-stage0-20260811/matrix-results/download-c4 \
  --sizes 16MiB --runs 3 --concurrency 4 --mode download

./build/bin/minikv_v2_bench local \
  --gateway 127.0.0.1:18521 \
  --work-dir /data/minikv-v2/v4-stage0-20260811/matrix-results/mixed-c4 \
  --sizes 16MiB --runs 3 --concurrency 4 --mode mixed
```

## download-only 结果

P50/P95/P99 为成功文件的下载完成时间，吞吐由 `16 MiB / P50` 换算。每个样本均在
计时后完成 SHA-256 校验。

| 文件并发 | 成功/请求 | 下载 P50 | 下载 P95 | 下载 P99 | P50 单流吞吐 |
|---:|---:|---:|---:|---:|---:|
| 1 | 3/3 | 194.370 ms | 218.071 ms | 218.071 ms | 82.32 MiB/s |
| 2 | 6/6 | 176.847 ms | 205.476 ms | 205.476 ms | 90.47 MiB/s |
| 4 | 12/12 | 178.288 ms | 207.316 ms | 207.316 ms | 89.74 MiB/s |
| 8 | 24/24 | 279.334 ms | 335.266 ms | 353.587 ms | 57.28 MiB/s |

解释：并发 1--4 下的文件级 P50 接近，说明 loopback、sendfile 及当前读取路径能并发服务
至少 4 个文件；并发 8 时所有请求仍成功，但尾延迟和单文件速度明显变差。这个拐点需要结合
EventLoop lag、磁盘队列与 outputBuffer 指标才能归因，不能仅凭该表判断为 epoll、磁盘或
客户端限制。

## mixed 结果

mixed 的每轮工作者一半上传、一半下载。上传和下载的 P50 分别只统计对应操作的成功样本；
`成功/请求` 为两个操作合计。

| 总文件并发 | 上传/下载工作者（每轮） | 成功/请求 | 上传 P50 | 下载 P50 | 失败解释 |
|---:|---:|---:|---:|---:|---|
| 2 | 1 / 1 | 6/6 | 297.428 ms / 53.79 MiB/s | 298.961 ms / 53.52 MiB/s | 无失败 |
| 4 | 2 / 2 | 12/12 | 242.282 ms / 66.04 MiB/s | 275.812 ms / 58.01 MiB/s | 无失败 |
| 8 | 4 / 4 | 18/24 | 1080.727 ms / 14.80 MiB/s | 1120.995 ms / 14.27 MiB/s | 6 个上传 routes 返回 503 |

并发 8 的 6 个失败全部记录为：

```text
Gateway POST ... /routes returned HTTP 503:
{"error":"no DataNode write capacity available"}
```

三轮中每轮有 4 个上传工作者，但双节点、双副本和每节点 2 个上传槽只能同时容纳 2 个文件，
因此每轮恰有 2 个上传被拒绝，共 6 个。12 个 mixed 下载工作者均完成 SHA-256 校验。

## smoke 与回归

- 三节点 smoke：download-only 并发 2 成功；修复对象名冲突后 mixed 并发 2 成功。
- benchmark 单元测试：`benchmark_types`、`benchmark_cli` 通过；包含 mixed 并发下限与
  新 CSV 格式验证。
- 本轮改动后的完整 CTest：45/46 通过；唯一失败为 Python 环境缺少 `playwright` 的前端 UI
  测试，与本轮数据面测试无关。

## 当前已经回答与仍未回答的问题

已得到证据：

- 双节点当前可以在写入同时服务下载；并发 4 mixed 全部成功。
- 并发 8 mixed 下，上传容量先触及 2 文件即时写入上限，Gateway 以可解释的 503 拒绝，而不是
  数据损坏。
- download-only 到并发 8 没有出现错误，但 P95/P99 上升。

仍不能从本报告推出：

- 下载的最大容量或 `maxActiveDownloads=8` 是否最优；测试只到双节点、loopback、16 MiB 与 3 轮。
- Chunk P50/P95/P99、建会话/routes/commit/manifest QPS 与延迟。
- EventLoop lag、CPU/RSS、上下文切换、`outputBuffer`、pause 时长、磁盘队列与 BlockPool 低水位。
- 跨机网络、真实故障、长时间稳定性，或 503 按 `Retry-After` 退避后的最终完成率。

## 下一项最小工作

本阶段下一项应是**可观测性**，不是立刻增加 epoll 线程：为 Chunk、控制面、EventLoop、
输出缓冲、pause 和磁盘执行器导出低基数聚合指标，并在相同的 download/mixed 矩阵中采集。
有了这些数据后，才能判断优先实施副本长连接池、Chunk window、数据块共享，还是读写公平调度。
