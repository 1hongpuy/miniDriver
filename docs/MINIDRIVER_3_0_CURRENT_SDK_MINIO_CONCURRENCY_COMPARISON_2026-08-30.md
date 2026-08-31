# MiniDriver 3.0：当前 SDK 与 MinIO 并发上传/下载对照（2026-08-30）

> 状态：同日开发机对照已完成。它比较并发曲线、量级和拐点，不是逐请求等价排名，也不是三主机或掉电 durability 验收。

## 1. 结论

在本机单盘、loopback、16 MiB 对象下：

- **上传：** MiniDriver SDK 的有限对象 aggregate 在 c16 附近达到约 **239.5 MiB/s**，c32 降至约 **218.5 MiB/s**；
  MinIO/Warp 持续 S3 PUT 在 c16 达到约 **328.2 MiB/s**，c32 仍约 **324.1 MiB/s**。
- **下载：** MiniDriver SDK 的严格 warm-GET 有效 aggregate 在 c16 约 **429.5 MiB/s**，c32 回落至约 **337.7 MiB/s**；
  MinIO/Warp warm GET 在 c8 达约 **2925.6 MiB/s**，c32 仍约 **2548.1 MiB/s**。
- MiniDriver 读侧的 c16→c32 已经是明确拐点；不要把默认客户端并发直接放到 32。MinIO 的数字则首先反映了其
  page-cache 命中和 Warp 消费模型，不可解释为“MiniDriver DataNode 只相当于 MinIO 的某个百分比”。

## 2. 环境与方法

```text
VM：8 vCPU，Ubuntu 22.04；网络：host/loopback
数据盘：/dev/sdc1 → /data-ssd，ext4
对象大小：16 MiB
并发：c1 / c4 / c8 / c16 / c32
```

### MiniDriver

```text
1 Gateway + 1 DataNode
identity：opaque chunk ID + CRC32C
PUT：buffered ACK，SDK Upload，有限对象；Session → Route/Capability → Chunk → Commit 都计入 aggregate
GET：warm fixture，SDK V3 ReadPlan、Keep-Alive、independent fixture；每 worker 5 次、5 轮
GET 校验：whole-chunk CRC32C，客户端下载到文件并进行 benchmark final SHA-256
```

### MinIO/Warp

```text
MinIO：quay.io/minio/minio:latest，单进程、单目录、host network
Warp：minio/warp:latest，连续压力；各输出实际报告 Ran: 5s
PUT：--disable-multipart --disable-sha256-payload
GET：warm existing objects，--list-existing --noclear；Warp 消费响应，不执行 MiniDriver 同样的拼接/落盘/final SHA
```

因此上传两侧都不是掉电 durability 证明；下载两侧客户端工作明显不同。所有 MiniDriver GET fixture 都在计时前上传，
没有清 Linux page cache。

## 3. 上传并发

MiniDriver 为 5 轮有限对象 aggregate 平均与 file-upload P50/P95；MinIO 为 Warp 持续压力 average 与 P50/P99。

| 并发 | MiniDriver aggregate | MiniDriver P50 / P95 | MinIO/Warp aggregate | Warp P50 / P99 |
|---:|---:|---:|---:|---:|
| 1 | 62.1 MiB/s | 126.3 / 138.1 ms | 143.3 MiB/s | 114.8 / 140.4 ms |
| 4 | 207.5 MiB/s | 137.8 / 176.4 ms | 309.9 MiB/s | 207.1 / 234.0 ms |
| 8 | 229.1 MiB/s | 326.6 / 540.6 ms | 310.0 MiB/s | 390.9 / 533.2 ms |
| 16 | **239.5 MiB/s** | 517.9 / 797.9 ms | **328.2 MiB/s** | 755.4 / 885.7 ms |
| 32 | 218.5 MiB/s | 1074.5 / 2086.4 ms | 324.1 MiB/s | 1573.3 / 1877.0 ms |

可作的判断：MiniDriver 当前单 DataNode 的有用上传工作点约在 **c8–c16**；c32 没有带来 aggregate 收益且 P95 明显恶化。
MinIO 仍在 c16–c32 平台区间。不能将两数相减为纯存储引擎差距，因为 MiniDriver 对每个有限对象计入控制面，Warp 是连续
S3 PUT；但该差距足以说明 MiniDriver 后续应继续拆解 Session/Route/Commit 和磁盘写路径。

## 4. warm 下载并发

MiniDriver 为 SDK strict client-visible aggregate 与 download P50/P95；MinIO 为 Warp average 与 P50/P99。

| 并发 | MiniDriver aggregate | MiniDriver P50 / P95 | MinIO/Warp aggregate | Warp P50 / P99 |
|---:|---:|---:|---:|---:|
| 1 | 87.4 MiB/s | 171.8 / 224.5 ms | 530.1 MiB/s | 30.8 / 50.2 ms |
| 4 | 338.6 MiB/s | 160.2 / 212.9 ms | 2488.0 MiB/s | 24.5 / 45.6 ms |
| 8 | 400.6 MiB/s | 235.1 / 288.4 ms | **2925.6 MiB/s** | 43.0 / 72.7 ms |
| 16 | **429.5 MiB/s** | 462.7 / 663.3 ms | 2819.0 MiB/s | 91.4 / 135.3 ms |
| 32 | 337.7 MiB/s | 1008.7 / 1524.5 ms | 2548.1 MiB/s | 200.5 / 294.4 ms |

这不是“DataNode 裸读吞吐”排名：MiniDriver 客户端在每次请求中还会执行 ReadPlan、Chunk 拼接、Chunk CRC、临时文件输出
和 final SHA-256；Warp 是高效 S3 消费客户端。该测试回答的是严格 MiniDriver SDK 对 LensCompute Worker 的当前体验：
**c8–c16 是实际读并发工作点，c32 已不适合作为默认。**

## 5. 原始证据与可复跑入口

```text
MinIO data： /data-ssd/minidriver-v3-minio-current-20260830/
MinIO logs： /tmp/minidriver-v3-minio-current-results-20260830/
SDK PUT：    /data-ssd/minidriver-v3-sdk-single-current-20260830/
SDK GET：    /data-ssd/minidriver-v3-sdk-read-current-20260830/
```

可复跑工具：

```text
tools/run_v3_minio_warp_baseline.sh
tools/run_v3_sdk_single_node_upload_matrix.sh
tools/run_v3_sdk_single_node_read_matrix.sh
```

## 6. 下一步

1. 为 MiniDriver 增加一个明确标注为 `transport-only` 的下载 benchmark：不落客户文件、不做 final SHA，但仍保留可选
   whole-chunk CRC；它用于隔离 SDK/服务端数据面，不能替代严格端到端 benchmark。
2. 在真实三主机、独立盘、真实 NIC 上重跑同一矩阵；单 VM 的 page cache、CPU 和 loopback 不可外推为分布式容量。
3. 对上传细分并量化 Session、Route、Chunk upload、File Commit；只有数据证明控制面占比显著，才优化或批量化它们。
4. 继续完成 group_commit/crash-recovery 验收；本报告的 buffered PUT 不构成 durable ACK 对照。
