# MiniDriver 3.0：Client SDK 性能验证与 MinIO 对照报告（更新至 2026-08-30）

> 状态：SDK 读写核心和本机代表性基准已完成；本报告不是 3.0 发布报告。真实三主机、严格冷缓存、掉电恢复和
> verified Range sidecar 仍未完成。

> 最重要结论：SDK 接入没有把 MiniDriver 的读写数据面变成另一套实现；benchmark 已通过 SDK 完成
> Upload → RF=2 Commit → Download → 最终 SHA-256 的端到端验证。单盘写入与 MinIO/Warp 已在同一天重测，
> 但二者协议、客户端校验和计时模型不同，只能比较数量级和并发拐点，不能作逐请求排名。

## 1. 本轮变更与验证边界

新 C++ SDK 位于 `include/client/`、`src/client/`：

```text
MiniDriverClient
  Upload: 创建一次 Session → 每 Chunk Route/Capability → 直传 Primary/Replica → Commit
  Read:   V3 ReadPlan 或 legacy Manifest → 直接 Chunk GET → 整 Chunk checksum → 有界 fallback
  Range:  对象 offset/length → 一个或多个 Chunk GET/Range；部分 Chunk 显式标记 unverified
  Transport: 一个 Worker 一个顺序 HTTP/1.1 Keep-Alive 连接；端点改变/对端关闭则重连
```

2026-08-29 的后续校正：当 Gateway 协商 `opaque-chunk-id + crc32c` 时，SDK 不再为了旧 V2 `hash` 字段计算
SHA-256；该字段改为稳定的 `upload:{sessionId}:{chunkIndex}` 路由键，真实物理身份仍由 Gateway 分配 `chunkId`。
只有显式 `sha256` / strong-content 模式才计算 SHA-256。这样 CRC 基线才不会把隐藏的客户端强哈希成本误计入数据面。

`minikv_v2_bench` 的上传和下载已经都调用该 SDK；旧 benchmark HTTP 类只是 SDK transport 的兼容别名。V3 模式不再
从 legacy manifest 猜测对象身份，而是把 Upload Commit 返回的 `objectId + objectVersion` 保存在 fixture 中并传给
`getReadPlan()`；这是 SDK 对 LensCompute Worker 的正确调用方式。

SDK 端到端集成用 1 Gateway + 2 DataNode、RF=2、16 MiB 对象（4 × 4 MiB Chunk）、2 个文件并发完成：

```text
4/4 objects success
每个对象：SDK Upload → Gateway File Commit → SDK Download → whole-file SHA-256
```

这次集成暴露并修复两项兼容问题：旧 V2 Manifest 没有逐 Chunk `size`，SDK 现按 `fileSize + chunkSize + index`
重建最后一个 Chunk 的长度；Route 收到 503 时 SDK 对相同 `sessionId + chunkIndex` 最多做 6 次有界重试，
不会创建第二个逻辑 Chunk。

随后使用全新的隔离集群显式验证了 V3 读取契约：2 轮 × 2 个 16 MiB 对象全部成功，流程为

```text
SDK Upload → Gateway Commit（ObjectRef） → POST V3 ReadPlan → Read Capability
→ DataNode V3 Chunk GET → CRC32C whole-chunk verification → whole-file SHA-256
```

两轮 aggregate 分别为 `73.64 MiB/s`、`62.46 MiB/s`；单次上传约 `77.46–103.89 MiB/s`，下载约
`80.33–106.47 MiB/s`。这是小规模**语义集成验证**，不是吞吐峰值；原始证据为
`/tmp/minidriver-v3-sdk-readplan-results-20260829f/`。

在去除 fast-crc32c 上传端 SHA-256 计算后，又用独立集群重跑相同的 V3/RF=2 验证：4/4 个 16 MiB 对象成功，
两轮 aggregate 为 `68.10`、`65.85 MiB/s`；单对象上传范围 `69.18–154.50 MiB/s`，下载范围
`80.95–96.37 MiB/s`。所有对象最后均由 benchmark 另行计算 whole-file SHA-256 验证。该 SHA 仅用于测试结果，
不属于 fast-crc32c 上传路由路径；原始 CSV 为 `/tmp/minidriver-v3-sdk-crc-e2e-results-20260830/`。

Gateway 的进程默认协议现为 `opaque-chunk-id + crc32c`。不设置协议环境变量的额外 RF=2 验证同样完成 4/4 个
4 MiB 对象的 SDK Upload → Commit ObjectRef → V3 ReadPlan → final SHA-256，原始结果为
`/tmp/minidriver-v3-sdk-default-e2e-results-20260830/`。

为保持 legacy 可测，benchmark 新增 `--upload-checksum crc32c|sha256`。SDK 将它与 Gateway Session 的协议字段比对；
不匹配直接失败，不会静默改变校验契约。新默认与显式 `cas-sha256 + sha256` 的两套 RF=2 烟测均覆盖
64 KiB/4 MiB/16 MiB，原始目录分别为 `/tmp/minidriver-v3-p5-default-protocol-20260830/` 与
`/tmp/minidriver-v3-p5-legacy-protocol-20260830b/`。

SDK 现另提供 `downloadRangeToFile(plan, offset, length, ...)`：它按 ReadPlan 的对象顺序映射为单个或多个
DataNode Chunk GET/Range 请求，检查 206/长度并保持同一静态副本候选的有界 retry。若范围仅覆盖完整 Chunk，SDK 可以用既有
whole-chunk checksum 标记 `verified`；只要涉及部分 Chunk，就返回 `unverified-partial`。这是刻意的正确性边界，直到
Gateway/DataNode 增加 segment checksum sidecar；`test_minidriver_client` 已覆盖 `Range: bytes=2-5`、206、输出字节和
该 unverified 结果，并覆盖一个跨两 Chunk 的对象 Range 重新拼接。

除 benchmark 外，已新增 `build/bin/minidriver_client_worker` 作为原生 Worker 参考入口。它只接受 Gateway、集群
Capability 身份和 `objectId + objectVersion`，内部调用 SDK `getReadPlan()` 后直接读取 DataNode；它不接受、也不会访问
DataNode extent/本地文件路径。2026-08-30 在全新 RF=2 临时集群运行的链路为：

```text
benchmark SDK Upload + Commit
  → runs.csv 的 object_id/object_version
  → minidriver_client_worker read
  → V3 ReadPlan + Read Capability + DataNode read
  → Worker output SHA-256 == input SHA-256
```

该例的对象为 `b0bc7943c3d1d46c08dd1fb4665c4db3/v1`、大小 4 MiB，输入与 Worker 输出均为
`d6b88e30282d1885afbf25af0371f66ebb8861bda514baa1adb3e4120d2675b2`；可复查证据在
`/tmp/minidriver-v3-sdk-worker-demo-20260830b/`。`runs.csv` 也因此新增 `object_id,object_version` 两列，让后续
LensCompute 任务可直接持久化版本化对象引用。

## 2. 环境与方法

```text
VM:       8 vCPU Intel Xeon Gold 6230R，约 15 GiB RAM，Ubuntu 22.04 / kernel 5.15
数据盘:   /dev/sdc1 → /data-ssd，ext4，约 98 GiB VHDX
网络:     loopback；不是跨机 NIC/RTT 测试
MiniDriver 读:  1 Gateway + 2 DataNode，RF=2，SDK 完整对象落 /tmp 并做最终 SHA-256
MiniDriver 写:  1 Gateway + 1 DataNode，buffered durability，SDK 上传，16 MiB 对象
MinIO:          单进程、单目录、host network、Warp S3/SigV4 client，16 MiB PUT
```

MiniDriver 的下载包含对象拼接、临时输出和最终 SHA-256；Warp GET/PUT 的客户端行为不同。MinIO PUT 使用
`--disable-multipart --disable-sha256-payload`。两侧都没有做掉电恢复实验，因此不能以这些数据声明同等 durable ACK。

原始证据：

```text
SDK e2e:      /tmp/minidriver-v3-sdk-e2e-20260829d/
SDK P6:       /tmp/minidriver-v3-sdk-p6-results-20260829/
SDK R5:       /tmp/minidriver-v3-sdk-r5-results-20260829/
SDK single DN (historic): /tmp/minidriver-v3-sdk-single-write-results-20260829/
SDK V3 plan: /tmp/minidriver-v3-sdk-readplan-results-20260829f/
SDK CRC e2e: /tmp/minidriver-v3-sdk-crc-e2e-results-20260830/
SDK default: /tmp/minidriver-v3-sdk-default-e2e-results-20260830/
SDK single DN (current):  /data-ssd/minidriver-v3-sdk-single-crc-20260830/
MinIO/Warp (historic):    /tmp/minidriver-v3-sdk-minio-results-20260829c/
MinIO/Warp (current):     /tmp/minidriver-v3-minio-sdk-crc-results-20260830/
```

## 3. SDK 后的读取结果（warm cache）

所有 fixture 均在正式计时前通过 SDK 上传，故下列为 warm-cache 结果。

### 3.1 Hot 100 MiB Object

| 并发 | 样本 | P50 / P95 / P99 | 每轮 aggregate |
|---:|---:|---:|---:|
| 1 | 2 | 970.4 / 984.7 / 984.7 ms | 101.5–104.6 MiB/s |
| 8 | 16 | 1640.0 / 1709.1 / 1709.1 ms | 468.0–474.0 MiB/s |

SDK 读取每个对象后仍要写客户端文件并计算最终 SHA；因此它同时测客户端输出/CPU，不能把约 474 MiB/s
解释成 DataNode 的孤立上限。

### 3.2 Random Objects，c8

| 大小 | 成功 | P50 / P95 / P99 |
|---:|---:|---:|
| 64 KiB | 16/16 | 3.9 / 5.4 / 5.4 ms |
| 1 MiB | 16/16 | 30.4 / 38.4 / 38.4 ms |
| 16 MiB | 16/16 | 249.4 / 301.5 / 301.5 ms |

### 3.3 Mixed Size，c16

| 大小 | 样本 | P50 / P95 |
|---:|---:|---:|
| 64 KiB | 7 | 9.7 / 10.4 ms |
| 150 KiB | 7 | 10.9 / 18.9 ms |
| 1 MiB | 6 | 41.6 / 49.2 ms |
| 16 MiB | 6 | 273.6 / 297.0 ms |
| 50 MiB | 6 | 564.2 / 610.3 ms |

当前 c16 下缩略图量级对象没有被 16/50 MiB 原图拖到数百毫秒；但样本数很小，不能替代长时混合负载 SLA。

### 3.4 Keep-Alive

64 KiB hot object、c16、2 轮、每 worker 20 次，共 640 次 SDK 对象读取：

```text
success                         640 / 640
P50 / P95 / P99                3.07 / 8.37 / 14.70 ms
DataNode connection opens      32
DataNode requests              640
DataNode connection reuses     608
```

即每 worker/轮一条连接，后续请求复用该连接；这验证的是 SDK 的连接语义，不代表 Gateway manifest 控制请求也被复用。

## 4. R5：慢读者隔离的真实结论

R5 使用同一 16 MiB hot object，一半普通客户端、一半限速为 2 MiB/s：

| 并发 | 普通/慢读者 | 普通 P50 / max | 慢读者 P50 / max | 成功 |
|---:|---:|---:|---:|---:|
| 8 | 4 / 4 | 77.9 / 99.4 ms | 1.925 / 1.938 MiB/s | 8/8 |
| 16 | 8 / 8 | 52.9 / 61.3 ms | 1.921 / 1.924 MiB/s | 16/16 |

DataNode 进程 RSS 峰值约 20–21 MiB，应用层 `output_buffer_peak_bytes=0`，没有出现应用内存无界增长。

但这**尚不足以勾选 R5 发布验收**：loopback 的 `sendfile` 很快把字节交给内核 socket buffer，DownloadLease 会在内核接受
完字节后释放，而慢客户端之后才从自己的 socket 接收缓冲中消耗。因此这证明“当前应用层没有积压”，不证明真实低带宽 NIC、
高 RTT 或拥塞下的 lease/output-watermark 公平性。R7 必须在真实三主机网络上重跑。

## 5. 单 DataNode SDK PUT 与 MinIO/Warp PUT

### 5.1 MiniDriver（当前 SDK，CRC32C、buffered、1 DataNode）

2026-08-30 重跑：16 MiB、每文件 window=2、global chunk budget=16、每档 3 轮；Gateway 采用
`opaque-chunk-id + crc32c`，SDK 不预计算 SHA-256。aggregate 是 benchmark 整轮时间，包含有限对象创建、
Session/Route/Commit 等控制开销；对象延迟是与持续 Warp 参考时的辅助指标。

| 并发 | aggregate 每轮均值 | SDK file-upload P50 / P95 |
|---:|---:|---:|
| 1 | 65.5 MiB/s | 121.6 / 142.8 ms |
| 4 | 243.4 MiB/s | 128.1 / 163.0 ms |
| 8 | **262.7 MiB/s** | 301.7 / 353.0 ms |
| 16 | 258.3 MiB/s | 496.6 / 718.1 ms |

吞吐在 c8 附近达到本轮最高值；c16 相比 c8 没有有效 aggregate 收益，P95 约翻倍。这与此前“增加并发不等于
更高有效吞吐”的结论一致。原始每轮 aggregate 为：c1 `63.06/68.82/64.59`、c4
`237.18/246.38/246.49`、c8 `240.87/286.45/260.76`、c16 `230.40/264.77/279.80 MiB/s`。

### 5.2 MinIO/Warp（同日单盘 S3 PUT）

2026-08-30 重跑：16 MiB、约 7–8 秒持续压力、`--disable-multipart --disable-sha256-payload`：

| 并发 | Warp aggregate | Warp P50 / P99 |
|---:|---:|---:|
| 1 | 153.2 MiB/s | 110.1 / 123.7 ms |
| 4 | **315.2 MiB/s** | 207.6 / 222.8 ms |
| 8 | 308.4 MiB/s | 441.1 / 536.0 ms |
| 16 | 294.8 MiB/s | 869.1 / 1541.1 ms |

本轮中，MinIO/Warp 的持续 PUT 最高值出现在 c4；MiniDriver SDK 的有限对象 aggregate 最高值出现在 c8。
这些是各自的观测结果，不构成逐请求排名：MiniDriver 每个对象经过 Session、每 Chunk Route/Capability、Commit，
而 Warp 是持续 S3 PUT 且关闭 payload SHA；二者的客户端工作、计时边界和 durability ACK 都不同。

两组数字**不能**直接相减后宣称某一方“快 X%”：一个是有限对象和整个 Session/Route/Commit 的计时，另一个是持续
S3 请求压测；两者都不是掉电恢复验证。当前 CRC SDK 的一个明确事实是：其上传热路径不再含客户端 SHA-256；benchmark
的 final SHA-256 仅用于端到端正确性验证。

### 5.3 分布式 RF=2 的独立结果（不与单盘 MinIO 混排）

同日另跑 `opaque-chunk-id + crc32c + group_commit`、2 DataNode、RF=2 的 16 MiB 上传矩阵。其每轮 aggregate 均值为：

| 文件并发 | RF=2 aggregate 均值 | file-upload P50 / P95 |
|---:|---:|---:|
| 1 | 56.5 MiB/s | 180.9 / 194.9 ms |
| 4 | 122.2 MiB/s | 387.2 / 480.8 ms |
| 8 | 135.8 MiB/s | 783.2 / 869.7 ms |
| 16 | 153.5 MiB/s | 1040.4 / 1341.3 ms |

该表的目的是记录当前**双副本 + group commit**的开发机容量，不是和单副本 MinIO 做对照。原始证据为
`/data-ssd/minidriver-v3-p5-ab-sdk-crc-20260830/`。

### 5.4 三种 ACK 模式的最小新 SSD 矩阵

2026-08-30 用同一 SDK、单 Gateway + 单 DataNode、新 SSD 和 `opaque-chunk-id + crc32c` 跑了每档一轮的
`buffered / group_commit / chunk_sync`。`group_commit` 使用默认的 8 MiB/8 items/2 ms 组参数；该工具此前未接受
`group_commit`，现已修正。结果的用途是证明三条路径均可运行并量化本机 ACK 成本，**不是** P95/SLA，也不是断电恢复证明。

| 模式 | 16 MiB c1：文件延迟 / 轮 aggregate | 64 MiB c4：文件 P50/P95 / 轮 aggregate |
|---|---:|---:|
| buffered | 99.7 ms / 80.0 MiB/s | 620.2 / 641.0 ms / 234.0 MiB/s |
| group_commit | 146.5 ms / 63.2 MiB/s | 1530.8 / 1549.7 ms / 120.3 MiB/s |
| chunk_sync | 138.8 ms / 59.1 MiB/s | 1222.8 / 1256.5 ms / 144.7 MiB/s |

三种模式均成功完成请求。`buffered` 的 ACK 只确认写入内核页缓存路径；`chunk_sync` 在每个 Chunk 的规定时点同步；
`group_commit` 共享一次同步，因此又受到批次形成/等待影响。实现级 `test_fast_data_store_durability` 另外验证了同步失败不发布
对象、未索引 extent 在重启时回收以及关停 drain；但没有模拟主机掉电、虚拟磁盘缓存或文件系统崩溃。原始 CSV 位于
`/data-ssd/minidriver-v3-durability-sdk-20260830/`，因此正式 release 前仍需独立的 crash/recovery 程序。

## 6. 已确认、未确认与下一步

已确认：

- SDK 已成为 benchmark 的读写数据访问面，V2 legacy 适配和 whole-chunk checksum 可用；
- SDK Upload 有 session-stable Route retry，SDK Read 有有界候选副本 retry、连接复用和统计；
- 独立 SDK 回归测试覆盖 V3 ReadPlan 的 cluster/service 身份头、Read Capability 传递、坏副本强校验拒绝和
  静态第二副本 fallback；
- SDK 迁移后代表性读取、混合尺寸、Keep-Alive、RF=2 legacy/V3-ReadPlan end-to-end 和 R5 请求成功率没有回归；
- 单 DataNode SDK 写吞吐在约 c8 达到本机工作点，继续加到 c16 主要增加尾延迟。

仍未确认，因而仍阻塞 MiniDriver 3.0 发布：

```text
V3 objectId + objectVersion ReadPlan 成为 Worker/benchmark 默认入口（当前已有显式开关和集成验证）
SDK opaque chunkId 上传、断线 body retry、Capability 过期/撤销集成测试
checksum segment sidecar 与 verified Range
R5 在真实 NIC/RTT/限速网络的资源边界
R7 三主机独立磁盘、真实 NIC/Tailscale、RF=2
30 分钟持续读写；buffered/group_commit/chunk_sync 的掉电恢复对照
```

因此本报告的下一步不是盲目增加线程或把 Gateway 改成 gRPC，而是完成 SDK 的 V3 默认 ReadPlan/Upload adapter、
错误矩阵、segment sidecar 与真实网络 Range 集成，再在三主机上用同一 SDK 重跑 R1/R3/R4/R5 和 durability matrix。

`group_commit` 的同步写性能、CPU/iowait 证据和优化边界见
[Group Commit 持久化性能判断报告](MINIDRIVER_GROUP_COMMIT_DURABILITY_BOTTLENECK_REPORT_2026-08-30.md)。
