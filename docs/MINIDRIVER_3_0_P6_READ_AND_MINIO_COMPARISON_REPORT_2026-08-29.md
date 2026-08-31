# MiniDriver 3.0：P6 并发读取与 MinIO 对照报告（2026-08-29）

> 状态：本机 SSD 的 P6 warm-read 基线已完成 R1/R2/R3/R4/R6；R5 慢读者隔离与 R7 三主机尚未完成，因而这不是 3.0 发布验收报告。
>
> 结论边界：本文不把 MiniDriver 与 MinIO 写成逐请求等价排名。两者共享同一台 VM、同一块 SSD 和 loopback，但客户端、协议、
> 完整性校验、对象生命周期和计时模型不同。本文的价值是给出数量级、拐点和下一步应测什么。

## 1. 测试环境与证据

```text
VM：8 vCPU（Intel Xeon Gold 6230R），15 GiB RAM，Ubuntu 22.04 / kernel 5.15
数据盘：/dev/sdc1 → /data-ssd，ext4，98 GiB VHDX
客户端输出：/tmp；避免和服务端数据目录混为同一目录
MiniDriver：1 Gateway + 2 DataNode，loopback，RF=2
P6 下载准入：maxActiveDownloads=64，maxDownloadsPerClient=64（仅为探索拐点）
MiniDriver read：每次完整对象下载到客户端文件，随后计算 SHA-256
```

此前同盘 `fio` 基线为约 `487.73 MiB/s`（64 KiB/QD8 direct write）和 `536.49 MiB/s`（1 MiB/QD32 direct read）。
它是块设备 I/O 对照，不是 HTTP 对象存储吞吐承诺。见
[既有基线报告](MINIDRIVER_PERFORMANCE_BASELINE_AND_BOTTLENECK_REPORT_2026-08-25.md)。

本轮原始证据位于运行机：

```text
MiniDriver P6 CSV：/tmp/minidriver-v3-p6-read-results-20260829/
MiniDriver P6 服务端数据：/data-ssd/minidriver-v3-p6-*-20260829/
MinIO/Warp 原始日志：/tmp/minidriver-v3-minio-results-20260829/
MinIO 服务端数据：/data-ssd/minidriver-v3-minio-baseline-20260829/
```

旧的 `/data-ssd/minidriver-*` 测试运行目录已在本轮前按用户授权清理；源码、文档和 `/tmp` 中的证据目录未被当作旧数据删除。

## 2. 方法与共同限制

### 2.1 MiniDriver P6

```text
R1：同一 100 MiB 对象，所有读者共享该 immutable object
R2：同一 4 MiB storage Chunk
R3：独立 64 KiB / 1 MiB / 16 MiB 对象
R4：64 KiB + 150 KiB + 1 MiB + 16 MiB + 50 MiB 同轮混合
R6：64 KiB / 150 KiB，每 worker 顺序读 20 次，close 对比 Keep-Alive
```

所有 P6 fixture 都在计时前上传，所以本报告的读结果是 **warm-cache**。没有清 Linux page cache，也没有足够权限把它称为严格
cold-read。R1 `c64` 仅跑一轮，标为容量 spot check，不能用于 P95/SLA。

### 2.2 MinIO/Warp

```text
MinIO：quay.io/minio/minio:latest，host network，单进程、单盘、无纠删码
Warp：minio/warp:latest 1.3.1，S3/SigV4，8 秒持续压力
PUT：16 MiB，--disable-multipart --disable-sha256-payload
GET：warm existing objects，16 MiB，Warp 客户端消费
```

Warp 是持续时间压测；MiniDriver 是有限对象、manifest + Chunk GET、落客户端文件并 SHA-256 的完整性压测。尤其是 GET：
MinIO/Warp 可以明显命中 page cache，且不做 MiniDriver 客户端同样的对象拼接与最终 SHA，因此不能直接相减得到“服务端快多少”。

## 3. P6 读侧结果

### 3.1 R1：100 MiB Hot Object

| 并发 | 样本 | 每对象下载 P50 / P95 / P99 | 已记录的轮聚合吞吐 | 说明 |
|---:|---:|---:|---:|---|
| 1 | 3 | 1129.8 / 1147.0 / 1147.0 ms | 87.2–92.0 MiB/s | 基线 |
| 4 | 12 | 1000.0 / 1167.7 / 1167.7 ms | 342.5–397.6 MiB/s | loopback 热缓存开始并行受益 |
| 8 | 24 | 1935.0 / 2030.4 / 2035.5 ms | 未单独保留每轮 aggregate 行 | 有完整 CSV；不能倒推成严格 aggregate |
| 16 | 48 | 3315.9 / 3495.3 / 3571.0 ms | 447.4–472.6 MiB/s | 吞吐接近本机工作点 |
| 32 | 96 | 9343.8 / 11697.5 / 11792.7 ms | 270.9–331.0 MiB/s | 客户端 SHA/输出与 VM CPU 竞争，吞吐下降 |
| 64 | 64，单轮 | 89348.2 / 97565.1 / 99493.5 ms | 64.3 MiB/s | **容量 spot check，非 3 轮结果** |

R1 不能用来宣称“DataNode 最高只能约 470 MiB/s”：客户端在每个完整对象后写临时文件并 SHA-256，c64 的 6.4 GiB
客户端工作量已证明这个基准同时测到了客户端 CPU/写入路径。它仍然有效地给出默认前端并发边界：本机热对象从 c16 往上尾延迟
急剧变坏，c32 已没有吞吐收益。

### 3.2 R2：4 MiB Hot Chunk

| 并发 | 成功 | P50 / P95 / P99 |
|---:|---:|---:|
| 1 | 3/3 | 65.5 / 95.6 / 95.6 ms |
| 4 | 12/12 | 74.6 / 119.6 / 119.6 ms |
| 8 | 24/24 | 89.2 / 132.4 / 132.4 ms |
| 16 | 48/48 | 156.6 / 205.6 / 212.9 ms |
| 32 | 96/96 | 259.0 / 359.8 / 383.5 ms |
| 64 | 192/192 | 506.3 / 697.1 / 735.7 ms |

375/375 成功说明下载准入、sendfile 和连接处理在临时 64 槽下没有出现错误；但 P50 在 c32→c64 接近翻倍，因此 c64 是容量验证而不是
合理默认值。

### 3.3 R3：Random Objects

| 对象大小 | c1 P50 / P95 | c8 P50 / P95 | c16 P50 / P95 | c32 P50 / P95 |
|---:|---:|---:|---:|---:|
| 64 KiB | 2.9 / 3.0 ms | 5.4 / 7.7 ms | 8.5 / 13.2 ms | 7.8 / 19.0 ms |
| 1 MiB | 22.6 / 24.3 ms | 34.6 / 49.6 ms | 49.6 / 80.5 ms | 70.7 / 195.3 ms |
| 16 MiB | 171.3 / 193.0 ms | 305.7 / 336.4 ms | 520.9 / 592.9 ms | 1038.9 / 1303.4 ms |

这是当前更可信的读侧默认依据：小对象 c16 仍可用；1 MiB 和 16 MiB 在 c32 的尾延迟已明显放大。后续浏览器/Worker
调度应从 `c8–c16` 量级起步，再根据真实网络与 CPU 指标调节，而不是直接放开 64。

### 3.4 R4：Mixed Object Size

R4 修正过一次：最初 c4 因 worker 固定映射而漏掉第 5 种对象；基准现按轮次轮转 fixture，完整多轮覆盖所有大小。

| 并发 | 64 KiB P95 | 150 KiB P95 | 1 MiB P95 | 16 MiB P95 | 50 MiB P95 |
|---:|---:|---:|---:|---:|---:|
| 4 | 4.5 ms | 7.2 ms | 21.7 ms | 163.9 ms | 471.3 ms |
| 8 | 5.5 ms | 14.6 ms | 35.1 ms | 246.3 ms | 559.3 ms |
| 16 | 14.1 ms | 21.2 ms | 67.6 ms | 318.2 ms | 587.5 ms |
| 32 | 26.8 ms | 44.0 ms | 119.0 ms | 602.9 ms | 1068.8 ms |

结论不是“大对象绝不影响缩略图”，而是当前隔离下 c16 仍把 64 KiB P95 控制在约 14 ms；到 c32 已升到约 27 ms。
R5 必须验证慢 TCP 消费者后，才能证明 output buffer 与下载槽真正隔离。

### 3.5 R6：Keep-Alive

每 worker 连续读取同一小对象 20 次、3 轮。DataNode 数据连接统计不包含每次 manifest 的 Gateway 控制请求。

| 大小/并发 | close P95 | keep-alive P95 | close 连接/请求 | keep-alive 连接/请求/复用 |
|---|---:|---:|---:|---:|
| 64 KiB c1 | 3.5 ms | 3.2 ms | 60/60 | 3/60/57 |
| 64 KiB c16 | 11.1 ms | 9.6 ms | 960/960 | 48/960/912 |
| 64 KiB c32 | 17.6 ms | 16.6 ms | 1920/1920 | 96/1920/1824 |
| 150 KiB c1 | 6.7 ms | 6.8 ms | 60/60 | 3/60/57 |
| 150 KiB c16 | 20.2 ms | 15.7 ms | 960/960 | 48/960/912 |
| 150 KiB c32 | 29.2 ms | 31.6 ms | 1920/1920 | 96/1920/1824 |

Keep-Alive 的**机制验证**已成立：同一 DataNode 连接可连续读请求，不出现响应串包，连接数从每请求一条降为每 worker/轮一条。
对 c32/150 KiB 的 P95 并非单调改善，说明连接建立已不是该档唯一成本；调度、CPU 与服务端输出竞争仍需单独观测。

## 4. 单节点写入与 MinIO/Warp 对照

### 4.1 同日 MiniDriver 单 DataNode

```text
1 Gateway + 1 DataNode，4 MiB Chunk，16 MiB object
buffered durability，pwritev 256 KiB/1 ms，upload admission=64
每档 3 轮，有限对象上传；没有 fsync/fdatasync ACK
```

| 并发 | MiniDriver 每轮 aggregate | MiniDriver 上传 P50 / P95 |
|---:|---:|---:|
| 1 | 48.7–60.8 MiB/s | 151.0 / 184.8 ms |
| 4 | 163.2–186.5 MiB/s | 201.9 / 222.3 ms |
| 8 | 206.0–217.8 MiB/s | 391.6 / 447.4 ms |
| 16 | 201.9–233.4 MiB/s | 741.8 / 831.0 ms |
| 32 | 200.2–227.1 MiB/s | 1576.8 / 1890.7 ms |

### 4.2 同日 MinIO/Warp 单盘 PUT

| 并发 | Warp PUT aggregate | Warp P50 / P99 |
|---:|---:|---:|
| 1 | 126.1 MiB/s | 125.0 / 151.6 ms |
| 4 | 241.7 MiB/s | 266.2 / 314.4 ms |
| 8 | 291.9 MiB/s | 449.4 / 570.6 ms |
| 16 | 263.7 MiB/s | 873.4 / 1395.3 ms |
| 32 | 280.2 MiB/s | 1846.4 / 2158.8 ms |

本轮数量级判断：MiniDriver c8 后稳定在约 200–233 MiB/s；MinIO/Warp 峰值约 292 MiB/s。差距值得继续剖析，
但**不能**解释成“MinIO 一定快 X%”：

- Warp 是连续 5–6 秒压力，MiniDriver 是每个有限对象经过 Session、Route、Chunk Commit 和 File Commit；
- Warp PUT 明确关闭了客户端 SHA-256 payload；MiniDriver 仍有自己的完整性/控制协议成本；
- 两者普通 PUT 都不能代替掉电实验，MiniDriver 本组尤其明确是 `buffered`；
- MinIO 的 S3 SDK/SigV4 路径与 MiniDriver 原生 HTTP 路径不是同一个 API 负载。

### 4.3 MinIO/Warp warm GET（仅作热缓存参考）

| 并发 | Warp GET aggregate | P50 / P99 |
|---:|---:|---:|
| 1 | 435.1 MiB/s | 35.6 / 65.5 ms |
| 4 | 2146.9 MiB/s | 28.6 / 53.1 ms |
| 8 | 2470.6 MiB/s | 50.7 / 78.4 ms |
| 16 | 2225.8 MiB/s | 113.5 / 168.3 ms |
| 32 | 1742.2 MiB/s | 287.5 / 410.9 ms |

这些数字远高于 SSD `fio` 冷/直接读基线，因此它们首先说明 page cache + HTTP 客户端路径很快，**不能**作为 SSD 或 MiniDriver
冷读的排名。MiniDriver R1 的客户端会落地并 SHA-256，而 Warp GET 的消费模型不同；二者应分别保留。

## 5. 新发现、已完成与未完成

已完成：

- `StreamingRequest` 支持端点安全的顺序 Keep-Alive；切换 DataNode 或失败会重连；
- 基准新增 `hot-object`、`mixed-size`、`requests-per-worker`，并输出 connection opens/requests/reuses；
- mixed-size fixture 轮转，防止低并发遗漏对象类别；
- Range、Read Capability、健康端点、Keep-Alive 单元测试已经有实现与基础测试；
- P6 R1/R2/R3/R4/R6 和同日 MinIO/Warp 原始数据已保存。

仍未完成，不能在 3.0 Done 中勾选：

```text
R1 strict cold cache（需受控 host procedure）
R5 slow-reader isolation：限速客户端 + RSS/output watermark/download lease 连续采样
R7 真实三主机：独立磁盘、真实 NIC/Tailscale、RF=2
30 分钟持续读写稳定性
Keep-Alive 的 max requests、idle timeout、server close-reason 指标
drop/retry/404/416 后的跨进程 Keep-Alive 回归
durable group_commit 与 MinIO 的独立掉电/恢复语义验证
```

副本感知读调度、DataLocalityHint 与 LensCompute 任务放置仍然是后续功能，不应伪装成本报告中的已测能力。

## 6. 下一步

1. 先实现并跑 R5：正常读者与限速读者同时运行，持续采样 DataNode RSS、output watermark、active downloads 和正常读 P99；
2. 在三台真实主机上重跑 R1/R3/R4 的代表档，明确跨机 RTT/NIC/独立 SSD 的影响；
3. 单独跑 `buffered / group_commit / chunk_sync` 的相同写矩阵和故障恢复，不混用吞吐；
4. 只有读侧真实指标表明单 Gateway 或候选副本选择饱和，才进入 4.0 的多 Gateway/Replica-aware read scheduling。

