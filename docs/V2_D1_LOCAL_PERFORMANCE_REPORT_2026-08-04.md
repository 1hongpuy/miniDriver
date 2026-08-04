# V2 D1 本机性能复测（2026-08-04）

## 目的

本轮验证 D1 的有界磁盘写入队列是否在不改变 V2 协议的前提下，保持正确的数据面
行为，并给出后续性能优化的干净基线。D1 的目标不是让单个上传“必然更快”，而是把
`SHA256_Update`、`pwrite`、本地 SHA 校验和索引提交移出 DataNode 的 EventLoop，使
磁盘抖动不会直接阻塞网络事件循环。

## 方法与有效性

每个并发档位均启动一个全新的本机隔离集群，并使用独立的 Gateway LevelDB 和两个
DataNode 数据目录：

```text
Gateway     127.0.0.1:1819x
DataNode A  127.0.0.1:1911x / 1912x / 1913x
DataNode C  127.0.0.1:1911x / 1912x / 1913x
```

每条上传为 16 MiB，即 4 个 4 MiB Chunk；每个档位运行 3 轮。每个文件完整经过：

```text
create session -> route -> direct Chunk PUT -> chain replica -> Gateway commit
-> manifest -> direct Chunk GET -> Chunk SHA-256 + full-file SHA-256 validation
```

关键约束：**不能复用前一个并发档位的数据目录。** Chunk 以 hash 内容寻址；若重用
目录，后续相同基准输入会命中已存在 Chunk，`pwrite` 被跳过，测到的是去重路径而不是
真实写盘路径。本报告的 C1、C2、C4 均按该规则重新启动了集群。

运行环境：4 vCPU、约 3.6 GiB 内存、本机 loopback 网络。它不代表 Tailscale、浏览器
或跨机网络带宽。测试期间 `/tmp` 剩余空间约 1.3 GiB，样本规模保持为 16 MiB。

## 结果

### C1：单文件并发

| 指标 | 结果 |
|---|---:|
| 成功数 | 3 / 3 |
| 上传 P50 | 59.546 ms |
| 上传 P95 | 60.176 ms |
| 单文件上传 P50 吞吐 | 268.70 MiB/s |
| 下载 P50 | 56.157 ms |
| 单文件下载 P50 吞吐 | 284.91 MiB/s |
| 端到端聚合吞吐 | 101.39 / 100.82 / 100.20 MiB/s |

### C2：两个文件并发

| 指标 | 结果 |
|---|---:|
| 成功数 | 6 / 6 |
| 单文件上传 P50 | 99.439 ms |
| 单文件上传 P95 | 110.631 ms |
| 单文件上传 P50 吞吐 | 160.96 MiB/s |
| 单文件下载 P50 | 60.190 ms |
| 单文件下载 P95 | 67.378 ms |
| 每轮端到端聚合吞吐 | 143.36 / 154.83 / 161.71 MiB/s |
| 聚合吞吐中位数，相对 C1 | 154.83 MiB/s，约 +54% |

单条流的延迟会因共享 CPU、磁盘 Worker 和副本链而增加；但总体吞吐提升，且 6/6
完整校验成功。这是 D1 当前最重要的并发收益：有限并发被工作队列吸收，网络 loop
不再直接执行落盘。

### C4：四个文件并发（容量边界）

| 指标 | 结果 |
|---|---:|
| 成功数 | 6 / 12 |
| 拒绝数 | 6 / 12 |
| 拒绝方式 | Gateway routes 返回 HTTP 503 |
| 成功流上传 P50 | 97.353 ms |
| 成功流下载 P50 | 56.474 ms |
| 成功流每轮聚合吞吐 | 154.28 / 145.39 / 159.32 MiB/s |

这不是数据损坏或死锁。当前每个 DataNode 的 `maxConcurrentWrites=2`，而每条上传的
副本链同时占用 A/C 两个节点的一个写入槽。因此两个文件并发已占满两个节点，额外两条
流应立即得到：

```json
{"error":"no DataNode write capacity available"}
```

这验证了写入准入的设计：系统在已知容量外明确拒绝，而不是无限缓存、OOM 或使
EventLoop 长时间无响应。客户端下一步应实现退避并重试，或者 V4 引入按节点容量调节
的并发窗口。

## 与旧报告的关系

[V2_RELEASE_RETEST_2026-07-29.md](V2_RELEASE_RETEST_2026-07-29.md) 中的旧 16 MiB
矩阵记录了同步落盘时期的历史现象，但测试目录复用和内容去重会影响后续样本，因此
不能严谨地把两份报告相减并宣称某个精确的 D1 性能提升百分比。

本报告应作为 D1 后的有效基线。后续在同一“每个档位新集群”的方法下复测，才可以做
严格的版本对比。

## 已覆盖与未覆盖的指标

本轮已覆盖：上传/下载 MiB/s、1/2/4 并发流、文件完成延迟、完整 hash 校验、链式
副本和写入准入边界。

本轮未给出控制面 QPS、Chunk P99、精确的 queue peak/pause 聚合、EventLoop 延迟和
新的火焰图：当前 `minikv_v2_bench` 没有导出这些聚合指标，且本机
`kernel.perf_event_paranoid=4`，普通用户无法重新采集 `perf`。旧火焰图仍可证明旧路径
的热点，但不能冒充 D1 后的火焰图。

要补齐这些数据，应在下一轮：

1. 让 benchmark 收集 Gateway route/commit/manifest 的时延和 QPS。
2. 从 DataNode 结构化日志聚合 `disk_queue_peak_bytes`、`disk_pause_count`、
   `disk_pause_ms` 与 EventLoop loop-lag。
3. 由管理员临时降低 `perf_event_paranoid` 或授予受控 `CAP_PERFMON`，使用
   `RelWithDebInfo` 重新采集火焰图。

## 复现

每个并发档位使用不同 root 和端口，关键是不能复用 DataNode 数据目录：

```bash
export MINIKV_V2_CLUSTER_SECRET=local-d1-benchmark-secret
export MINIKV_V2_BIN_DIR="$PWD/build/bin"
export MINIKV_V2_BENCH_CLUSTER_DIR=/tmp/minikv-v2-d1-cluster-c2
export MINIKV_V2_BENCH_GATEWAY_PORT=18192
export MINIKV_V2_BENCH_NODE_A_PORT=19122
export MINIKV_V2_BENCH_NODE_C_PORT=19123

tools/start_v2_local_benchmark_cluster.sh
./build/bin/minikv_v2_bench local \
  --gateway 127.0.0.1:18192 \
  --work-dir /tmp/minikv-v2-d1-results/c2 \
  --sizes 16MiB --runs 3 --concurrency 2 --remote-dir /benchmark-d1-c2
MINIKV_V2_BENCH_CLUSTER_DIR=/tmp/minikv-v2-d1-cluster-c2 \
  tools/stop_v2_local_benchmark_cluster.sh
```

临时集群与 benchmark 输入均在 `/tmp`，报告写入后可删除，不能删除仓库源码或真实运行
数据目录。
