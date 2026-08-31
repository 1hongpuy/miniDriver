# MiniDriver 3.0：SDK 下载隔离实验报告（2026-08-31）

> 状态：开发机单 DataNode、warm-cache 的诊断实验。它用于解释严格 SDK 下载在 c16 的尾延迟，不能外推为真实三主机容量。

## 1. 问题与方法

此前严格 SDK 下载在 c16 的 P95 明显高于 c8。为区分 DataNode/SDK 传输路径与客户端完整对象落文件、最终 SHA-256
的影响，本轮在相同的 V3 ReadPlan、Keep-Alive、16 MiB 对象（4 x 4 MiB Chunk）、CRC32C、c8/c16 下比较两种模式：

```text
strict：         Chunk CRC32C → 对象拼接并落 /tmp → benchmark final SHA-256
transport-only：Chunk CRC32C → 丢弃已验证 Chunk；不写对象文件、不做 final SHA-256
```

`transport-only` **没有关闭校验**：仍验证每个完整 Chunk 的 CRC32C，也仍执行 Gateway ReadPlan、Read Capability、
DataNode HTTP/Keep-Alive 和候选副本逻辑。它只是一个诊断模式，不能替代用户可见的严格下载验证。

每档为 3 轮，每 Worker 顺序读 5 次；每轮在 fixture 准备后计时。每个并发档使用独立端口，避免新旧集群监听端口竞争。

## 2. 结果

| 模式 | 并发 | 轮 aggregate 均值 | 下载 P50 / P95 / P99 | 成功 |
|---|---:|---:|---:|---:|
| strict | c8 | 552.1 MiB/s | 198.1 / 237.4 / 263.7 ms | 120/120 |
| strict | c16 | 426.4 MiB/s | 400.6 / 531.2 / 666.4 ms | 240/240 |
| transport-only + CRC32C | c8 | 1377.8 MiB/s | 88.1 / 112.9 / 126.7 ms | 120/120 |
| transport-only + CRC32C | c16 | 1382.6 MiB/s | 165.3 / 263.1 / 341.1 ms | 240/240 |

两个明确结论：

1. 严格客户端下载中的对象文件输出与 final SHA-256 是大头。c8 下去掉它们后 aggregate 约为严格模式的 2.5 倍；c16 下约为
   3.2 倍。这解释了为何严格测试不能被当成 DataNode 裸读吞吐。
2. 这并不表示 c16 可以成为默认。transport-only 从 c8 到 c16 的 aggregate 几乎不增加，但 P95 从 112.9 ms 升至
   263.1 ms，说明 SDK/DataNode/loopback 路径本身也在 c8 后进入排队区间。严格模式会把该排队再与文件输出、SHA CPU 和
   `/tmp` 写入竞争叠加，形成 c16 的 531.2 ms P95。

因此当前默认仍应为 **下载 c8**；c16 仅适合作为可容忍更高延迟的批处理实验档，不能以 transport-only 峰值替换严格用户体验。

## 3. CPU、内存与调度证据

运行机没有安装 `pidstat`/`iostat`，本轮改为每秒记录 Gateway、DataNode、benchmark 的 `ps` CPU/RSS、`/proc/<pid>/status`
上下文切换及全机 `vmstat 1`。采样覆盖 fixture 准备与下载，因此不应把单秒峰值误写成仅下载阶段均值。

读阶段附近可观察到：

```text
Gateway：约 2% CPU，RSS 约 13 MiB
DataNode：约 27%–47% CPU，RSS 约 21 MiB
benchmark：约 100%–324% CPU，RSS 峰值约 105–205 MiB
```

这表明严格模式的客户端确实会消耗多个 CPU 核；同时 DataNode 和 Gateway 并未独占 8 vCPU。全机 `vmstat` 在样本中既有
约 74%–81% idle 的秒，也有 strict c16 的短时约 1%–2% idle 的秒，反映 fixture/文件写入和多 Worker 峰值叠加。
没有 `iostat`，所以本轮不能把任何一个 P95 精确归因成磁盘 `await`。

## 4. 变更、验证与后续

新增 SDK `downloadToSink()` 与 benchmark 参数：

```text
--download-verification strict|transport-only
```

`strict` 保持原有语义。`transport-only` 仍要求完整 Chunk CRC32C 和 ReadPlan 长度一致，只跳过对象文件输出与 benchmark
final SHA。`test_benchmark_cli` 和 `test_minidriver_client` 均已通过。

后续应补充：

1. 用 phase marker 把 fixture 上传、读阶段、客户端 SHA、客户端落盘分别计时；
2. 安装 `sysstat` 后采集 `pidstat -dur` 和 `iostat -x`；
3. 在真实三主机、独立 SSD、真实 NIC 下重跑 strict 与 transport-only c8/c16；
4. 保留 strict 作为用户可见可靠读取指标，transport-only 只用于容量归因。

## 5. 原始证据

```text
strict（有效）：
  /data-ssd/minidriver-v3-sdk-read-isolation-strict-valid-20260831/

transport-only（有效）：
  /data-ssd/minidriver-v3-sdk-read-isolation-transport-valid-20260831/
```

早先复用端口的两组尝试包含 `Acceptor bind failed`，明确不纳入本报告的数值结论；有效矩阵为每档独立端口后的上述目录。
