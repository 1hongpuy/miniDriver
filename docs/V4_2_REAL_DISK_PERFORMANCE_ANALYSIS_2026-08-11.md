# V4.2 真实磁盘性能与不足分析（2026-08-11）

## 结论

当前 V4.2 在双 DataNode、双副本、16 MiB 文件的真实 `/data` 磁盘环境中，
能够稳定支持 1 和 2 个并发文件上传/下载。默认准入下，4 和 8 并发中的额外
上传会在 Gateway 路由阶段得到可重试的 `503`，未发现 Chunk 校验错误或异常 `400`。

双 I/O EventLoop 在并发 2 上传上有小幅收益，但并未使整体吞吐翻倍；现阶段瓶颈
不应简单归因于单 EventLoop。

## 环境与口径

- 提交：`19f7b2e feat: add V4 multi-reactor datanode`
- 构建：`RelWithDebInfo`，GCC 11.4，CMake 3.22
- CPU：8 vCPU；内存约 2.8 GiB；Swap 2 GiB
- 数据盘：`/data`，ext4，约 491 GiB，总可用空间约 464 GiB
- 部署：Gateway 1 个、DataNode 2 个、loopback、链式双副本
- 对象：16 MiB，每文件 4 个 4 MiB Chunk；每档 3 轮
- 每档使用独立 Gateway/DataNode 数据目录；成功样本均下载并完成文件 SHA-256 校验
- 默认治理：每 DataNode `maxActiveUploads=2`，因此双副本链的有效并发写入上限为 2 个文件

基准命令形式：

```bash
MINIKV_V4_IO_THREADS=<0|2> tools/start_v2_local_benchmark_cluster.sh
./build/bin/minikv_v2_bench local \
  --gateway 127.0.0.1:<port> \
  --work-dir /data/minikv-v2/v4-perf-20260811-fMiSq5/results-io<loops>-c<concurrency> \
  --sizes 16MiB --runs 3 --concurrency <1|2|4|8>
```

## 正确性与可靠性

- 宿主环境 CTest：45/46 通过；唯一失败是 Python 环境未安装 `playwright`，不是数据面回归。
- `event_loop_thread`、`event_loop_thread_pool`、`tcp_server_multi_reactor`、
  `http_server_multi_reactor` 各重复 100 次，全部通过。
- 并发 1/2 的成功对象全部完成端到端 SHA-256 校验。
- 并发 4/8 的失败均是 Gateway routes 返回 `503 + Retry-After`，没有发现 hash 不匹配、复制失败或异常 400。

## 性能结果

文件级 P50；吞吐由 `16 MiB / P50` 换算。并发 4/8 的 P50 仅针对成功流。

| I/O loops | 文件并发 | 成功/请求 | 上传 P50 | 下载 P50 | 解释 |
|---:|---:|---:|---:|---:|---|
| 0 | 1 | 3/3 | 314.207 ms / 50.92 MiB/s | 146.587 ms / 109.15 MiB/s | 单流基线 |
| 0 | 2 | 6/6 | 291.691 ms / 54.85 MiB/s | 176.796 ms / 90.50 MiB/s | 可靠完成 |
| 0 | 4 | 6/12 | 240.010 ms / 66.66 MiB/s | 169.746 ms / 94.26 MiB/s | 每轮 2 个受控 503 |
| 0 | 8 | 6/24 | 245.456 ms / 65.18 MiB/s | 162.045 ms / 98.74 MiB/s | 每轮 6 个受控 503 |
| 2 | 1 | 3/3 | 344.622 ms / 46.43 MiB/s | 150.872 ms / 106.05 MiB/s | 无单流收益 |
| 2 | 2 | 6/6 | 277.360 ms / 57.69 MiB/s | 168.296 ms / 95.07 MiB/s | 上传 P50 相对 0 loop 约 +5% |
| 2 | 4 | 6/12 | 240.000 ms / 66.67 MiB/s | 160.484 ms / 99.70 MiB/s | 每轮 2 个受控 503 |
| 2 | 8 | 6/24 | 277.217 ms / 57.72 MiB/s | 187.117 ms / 85.51 MiB/s | 每轮 6 个受控 503 |

原始 CSV：`/data/minikv-v2/v4-perf-20260811-fMiSq5/results-*`。
`results-io0-c1` 是测试子进程被环境回收的无效首跑；本报告使用
`results-io0-c1-rerun`。

## 分析

1. 当前“并发 4/8 不全成功”是容量策略，不是传输损坏。两个节点各有 2 个写槽，
   一个双副本文件同时占用两个节点各一个槽，因此最多同时容纳两个文件。
2. Multi-Reactor 已通过生命周期与连接归属验证，但当前文件内 Chunk 仍串行，
   副本仍为每 Chunk 新建 HTTP/TCP 短连接。它无法单独消除副本、哈希和磁盘路径成本。
3. 并发 2 下 `io_threads=2` 有小幅上传收益；并发 1 和 8 没有稳定收益，说明
   下一项工作应由指标决定，而不是预设“增加 EventLoop 就会更快”。

## 当前不足与未覆盖项

- benchmark 只覆盖“一个文件上传完成后下载”，不支持真正上传/下载混合负载。
- 单文件 Chunk 发送串行，不能证明并行 Chunk window 的收益。
- 未导出 Chunk P50/P95/P99、routes/commit/manifest QPS、EventLoop lag、CPU/RSS、
  outputBuffer 峰值和磁盘 BlockPool 可用量。
- 没有本轮火焰图；不能用旧版本火焰图解释 V4.2。
- 4/8 的请求不会按 `Retry-After` 自动退避重试，因此它们反映“即时准入容量”，不反映排队后的最终完成率。
- 环境为 loopback 单机双进程，不能代表跨机网络、真实机架延迟或多节点故障域。

## 后续优化前的最小工作

先补 benchmark/日志聚合：混合负载、Chunk 延迟、控制面时延、EventLoop lag、
磁盘队列和 pause 指标。获得这些证据后再排序优化项：副本长连接池、受治理的
并行 Chunk window、SharedBodyBlock/writev，或读写公平调度。
