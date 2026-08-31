# MiniDriver V3 当前数据面性能基线与瓶颈定位报告（2026-08-25）

> 状态：8 槽位本机完整上传、下载、混合、小/大对象与控制面基线已完成；新增 SSD 上的同参数复测已完成；同盘
> MinIO/Warp 与 MiniDriver 单 DataNode 对照基线已完成；第一轮可观测性与可调参数已实现；MiniDriver 3.0
> `buffered/chunk_sync` 真实持久化对照与 P3 `pwritev` 聚合写验收已完成。  
> 范围：当前 `v3` 分支的数据面与 V2 HTTP API 保持兼容，尚未开始 Raft/Metadata 高可用迁移。  
> 结论：短负载下，当前最先限制混合高并发上传体验的是**每个 DataNode 默认仅 2 个前台上传 Chunk 槽位**及客户端的
> `global-chunk-budget`；8 槽扫描把本机页缓存突发吞吐提高到约 `190 MiB/s`。但新增的 2.5 GiB 持续上传与直接 I/O
> 标定证明，这个数字不是物理盘持续能力：两个本机 DataNode 共享当时枚举为 `/dev/sdb2` 的系统盘，RF=2 在约
> 3 GiB 脏页阈值后触发内核
> 回写节流，聚合逻辑吞吐降至约 `61 MiB/s`；同盘双路 64 KiB 非零数据直接写的物理上限约 `117 MiB/s`，折算 RF=2
> 逻辑上限约 `58.5 MiB/s`，两者吻合。因此 8 槽仍可作为突发 admission 候选，但当前持续写瓶颈首先是**两个副本
> 共用同一虚拟磁盘**，不是 Gateway 路由、loopback TCP 或全机 SHA 算力。三机独立磁盘复测前，不应把短测
> `190–200 MiB/s` 当成持续或断电持久化吞吐。
>
> 将整个隔离测试集群迁移到新增 SSD 后，16 MiB 短时上传仍约 `180–195 MiB/s`，普通混合负载也仍约
> `270–280 MiB/s`，说明这两个突发平台并非由旧盘单独造成；但同参数 5 GiB RF=2 持续写入没有再出现旧盘第 7 轮
> 跌到约 `61 MiB/s` 的断崖，P95 从约 `3.30 s` 降到 `1.12 s`。因此新盘首先改善的是持续回写和大对象并发长尾，
> 当前 HTTP/复制协议路径、SHA-256、RF=2 复制链和浅磁盘队列仍限制短时应用吞吐。
>
> 同一新 SSD 上，官方 MinIO 单盘经 Warp 测得 16 MiB PUT **应用确认吞吐**峰值约 `275.43 MiB/s @ c16`；MiniDriver
> 单 DataNode、单副本测得约 `234.75 MiB/s @ c16`，差约 17%，两者均在继续加并发后回落。这个结果说明 MiniDriver 不是比成熟
> 对象存储慢一个数量级，当前差距主要集中在小对象固定开销、数据复制与应用 I/O 管线利用率；它也不能把单副本
> 成绩冒充 RF=2 或断电持久化成绩。本轮没有为两者建立“响应前数据和 metadata 均完成强制持久化”的等价测试，
> 所以下文所有 PUT 数字默认都不是 durable throughput。

## 1. 本轮回答的问题

本轮不是宣称“高并发已经做好”，而是先回答四件可以测量的事：

```text
1. 默认配置下，上传、下载、混合负载的端到端延迟是多少？
2. 并发超过容量时，是资源无界增长/数据错误，还是明确且可恢复地被限流？
3. 长尾主要发生在 Gateway、DataNode 磁盘执行器，还是上传准入与客户端排队？
4. 将容量从 2 提到 4 后，方向是否值得继续做严格标定？
```

回答是：默认保护机制正确地限制了资源，但容量配置过于保守，且客户端对 `503` 没有退避重试；磁盘任务的
观测数据没有显示出 executor 队列已经成为秒级尾延迟的来源。

## 2. 测试环境与边界

| 项目 | 本轮环境 |
|---|---|
| CPU | 8 logical CPUs，Intel Xeon Gold 6230R |
| 内存 | 15 GiB（压测开始时约 13 GiB 可用） |
| OS | Ubuntu / Linux `5.15.0-190-generic` |
| 旧盘基线 | `/tmp`，测试时枚举为 `/dev/sdb2`、当前为 `/dev/sda2`，ext4；新增磁盘后设备名发生过变化 |
| 新盘复测 | `/data-ssd`，`/dev/sdc1`，ext4，约 98 GiB；Gateway、两个 DataNode 和 benchmark work-dir 均迁入该挂载点 |
| 拓扑 | 单机 loopback：1 Gateway + 2 DataNode，RF=2 |
| 对照拓扑 | 同一 `/dev/sdc1`：MinIO 单盘单实例；MiniDriver 1 Gateway + 1 DataNode、单副本 |
| 数据 | 每文件 16 MiB，按 4 MiB Chunk 分成 4 块 |
| 网络 | `127.0.0.1`，不代表真实 Tailscale、NIC、跨机磁盘或公网表现 |
| I/O reactor | `MINIKV_V4_IO_THREADS=2` |
| 写入持久性边界 | 当前 FastDataStore 使用 `pwrite`，没有 `fsync/fdatasync`；LevelDB `WriteOptions.sync=false`；结果包含 Linux page cache，不代表断电持久化吞吐 |

因此本报告可以定位**进程内排队、准入与协议路径**，但不能给出三台机器上的网络吞吐承诺，也不能替代长时间
稳定性、冷缓存、断电持久性或真实 SSD/磁盘阵列压测。当前“File Commit 成功”证明 RF=2 数据路径和应用层确认完成，
不能仅凭本报告宣称两个副本都已经强制落入非易失介质。

本报告严格区分三层完成语义：

```text
应用确认：pwrite / LevelDB Write 返回成功，HTTP 返回成功；数据仍可能只在 page cache。
内核回写：/dev/sdc 写扇区增加，说明部分脏页被异步提交给块设备；不证明某个对象在响应前完成。
durable commit：数据、必要 metadata 和它们的顺序均在响应前通过 fsync/fdatasync 或等价机制确认。
```

当前 MiniDriver 只实现第一层，压测期间会发生第二层，但没有第三层。进程正常退出、等待回写或测试结束后执行 `sync`
都不能倒推“之前每次 File Commit 返回时已经 durable”；掉电或内核崩溃时，已返回成功的最近对象及其索引仍可能丢失
或不一致。

压测原始结果保留在本机临时目录：

```text
/tmp/minidriver-perf-20260825/baseline/
/tmp/minidriver-perf-overload-20260825/
/tmp/minidriver-perf-slots4-20260825-rerun/
/tmp/minidriver-perf-observe-20260825/
/tmp/minidriver-slot-scan-{4,8,12,16}-20260825/
/tmp/minidriver-slot-scan-16-mixed-20260825/
/tmp/minidriver-v3-slots8-full-20260825/
/tmp/minidriver-perf-newssd-evidence-20260825/
/tmp/minidriver-minio-bench-evidence-20260825/
/tmp/minidriver-single-node-evidence-20260825/
```

它们不在仓库或 `data/` 中，也没有使用现有运行数据。

### 2.1 测试工作负载数据：并发数的精确定义

本 benchmark 的 `--concurrency` 是**本轮同时启动的总 worker 数**，不是“上传数”。模式含义固定如下：

```text
upload：   全部 worker 都上传。
download： 全部 worker 都下载。
mixed：    floor(concurrency / 2) 个下载 worker，剩余为上传 worker。
          因而 mixed c=8 = 4 上传 + 4 下载；mixed c=4 = 2 上传 + 2 下载。
```

混合模式会在正式计时前顺序上传每个下载 worker 所需的 fixture；fixture 创建不与正式混合负载并发，也不计入下表的
延迟样本。正式计时阶段的下载与上传才同时开始。

| 测试 ID | mode | 轮数 | 同时上传 | 同时下载 | 每轮总客户端 | 上传/下载延迟样本 | 文件与 Chunk | 每上传窗口 | 全局 Chunk 预算 |
|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| U1 | upload | 5 | 1 | 0 | 1 | 5 / 0 | 16 MiB / 4 × 4 MiB | 2 | 2 |
| U2 | upload | 5 | 2 | 0 | 2 | 10 / 0 | 16 MiB / 4 × 4 MiB | 2 | 2 |
| U4 | upload | 5 | 4 | 0 | 4 | 20 / 0 | 16 MiB / 4 × 4 MiB | 2 | 2 |
| D1 | download | 5 | 0 | 1 | 1 | 0 / 5 | 16 MiB / 4 × 4 MiB | - | 2 |
| D4 | download | 5 | 0 | 4 | 4 | 0 / 20 | 16 MiB / 4 × 4 MiB | - | 2 |
| M4 | mixed | 5 | 2 | 2 | 4 | 10 / 10 | 16 MiB / 4 × 4 MiB | 2 | 2 |
| M8 | mixed | 5 | 4 | 4 | 8 | 20 / 20 | 16 MiB / 4 × 4 MiB | 2 | 2 |
| O4（过载） | upload | 5 | 4 | 0 | 4 | 20 / 0 | 16 MiB / 4 × 4 MiB | 2 | 4 |
| M8-4（容量组） | mixed | 5 | 4 | 4 | 8 | 20 / 20 | 16 MiB / 4 × 4 MiB | 2 | 4 |

`global-chunk-budget` 是同一 benchmark 进程内、所有上传文件共用的已在途 Chunk 上限；它不是每个上传客户端的
上限。M8 的 4 个上传客户端各自最多有 2 个 Chunk window，但全局最多只有 2 个 Chunk 真正向服务端推进。

为避免这个歧义，新的 benchmark 输出已将 `upload_workers_per_round`、`download_workers_per_round`、样本数、
`chunk_window` 与 `global_chunk_budget` 写入 `summary.csv` 和 `summary.json`。

### 2.2 当前默认配置的容量边界（文件级与 Chunk 级分开）

`NodeResourceGovernor` 在每个 `ChunkUploadStream` 创建时获取 UploadLease，因此 `maxActiveUploads` 的单位是
**正在传输的 Chunk HTTP 请求**，不是完整文件。每个 16 MiB 文件有 4 个 Chunk；一个文件可同时提交多少 Chunk
由 `chunk-window` 决定。

以下是使用全新本机集群、1 轮的边界测试。这里的“成功”表示一次突发请求中完成了完整文件并校验成功；当前
benchmark 对 503 不重试，所以它测的是即时接纳容量，不是排队后的最终完成数量。

| 测试 | 突发上传 / 下载文件请求 | chunk window / budget | 完整上传成功 | 下载成功 | 直接结论 |
|---|---:|---:|---:|---:|---|
| U8-W2 | 8 / 0 | 2 / 8 | 1 | - | 2 个 Chunk 槽位被一个文件的两个在途 Chunk 占满 |
| U8-W1 | 8 / 0 | 1 / 8 | 2 | - | window=1 时可即时接纳 2 个不同文件 |
| D20 | 0 / 20 | - | - | 16 | 两节点读取均衡时为 8 + 8；其余 4 个为 HTTP 503 |
| M20-W2 | 10 / 10 | 2 / 10 | 1 | 10 | 已验证至少可同时完成 1 上传 + 10 下载 |
| M32-W2 | 16 / 16 | 2 / 16 | 0 | 16 | 不用于判定混合极限：突发路由无 503 重试，上传在首轮路由竞争中全部失败 |

因此，默认配置下当前可以精确表述为：

```text
单个 DataNode：最多 2 个上传 Chunk + 8 个下载请求同时占用配额。
两个 DataNode、RF=2：一个上传 Chunk 链会占用两个节点各一个上传配额，
                         整体最多 2 个并行复制 Chunk。
文件级：window=2 时即时接纳 1 个上传文件；window=1 时即时接纳 2 个上传文件。
下载级：已实测两个节点合计即时成功 16 个下载。
混合级：已实测 1 上传文件 + 10 下载；理论配额可同时容纳 2 个复制 Chunk + 16 下载，
         但尚不能称为“已验证的 1 上传 + 16 下载”，直到 benchmark 补 503 退避重试。
```

这也说明“同时请求 32 个客户端”不等于“32 个请求都会被立刻接纳”。前者是压测发起量，后者才是系统当前资源上限。

## 3. 当前数据面与被测语义

```text
Browser/benchmark
  → Gateway：Session、Route、Lease、Commit（小型控制请求）
  → DataNode primary：HTTP Chunk Body
  → DataNode replica：链式副本
  → primary：Gateway CommitChunk

下载：Browser/benchmark → DataNode sendfile
```

这很重要：文件 Body 不经过 Gateway。Gateway 的 `route_plan` 日志为内部处理时间，不等同于整个浏览器请求时间。

默认 DataNode 资源界限如下：

```text
maxActiveUploads        = 2 / DataNode
maxUploadsPerClient     = 2
DiskWriteExecutor       = 2 worker + 128 × 64 KiB block（8 MiB block pool）
Chunk pipeline watermark = 1 MiB high / 512 KiB low
```

由于此轮 RF=2 且仅有两个 DataNode，一个上传 Chunk 同时占用 primary 与 replica；在这个特定两节点拓扑中，
`maxActiveUploads=2` 实际上限制的是两个并行复制 Chunk。若文件使用 `chunk-window=2`，一个文件就会占满它；
若使用 `chunk-window=1`，可即时接纳两个文件。它是安全界限，不是吞吐最优值。

## 4. 默认配置基线

所有条目均为 16 MiB 文件。为避免混合测试歧义，样本数按“上传 / 下载”分别列出；`P95/P99` 在 5、10 或 20 个
同类样本下只用于方向判断，不能当作生产 SLA。

| 测试 ID | 同时上传 / 下载 | 上传/下载样本 | 上传 median / P95 / P99 (ms) | 下载 median / P95 / P99 (ms) | 上传/下载成功 |
|---|---:|---:|---:|---:|---:|
| U1 | 1 / 0 | 5 / 0 | 143.7 / 162.7 / 162.7 | - | 5 / - |
| U2 | 2 / 0 | 10 / 0 | 200.3 / 270.2 / 270.2 | - | 10 / - |
| U4 | 4 / 0 | 20 / 0 | 273.6 / 448.1 / 490.8 | - | 20 / - |
| D1 | 0 / 1 | 0 / 5 | - | 159.0 / 171.3 / 171.3 | - / 5 |
| D4 | 0 / 4 | 0 / 20 | - | 205.6 / 240.7 / 247.8 | - / 20 |
| M4 | 2 / 2 | 10 / 10 | 138.7 / 241.6 / 241.6 | 188.6 / 232.4 / 232.4 | 10 / 10 |
| M8 | 4 / 4 | 20 / 20 | 450.3 / 1831.8 / 1833.9 | 251.2 / 308.1 / 338.9 | 20 / 20 |

重复的、带新观测字段的 M8 运行得到：4 上传 + 4 下载同时运行，各有 20 个样本；上传 median/P95/P99 为
`406.9 / 533.6 / 534.3 ms`，下载为 `244.8 / 304.2 / 307.1 ms`，上传和下载各 `20/20` 成功。

两次 `mixed c=8` 的上传 P95 有明显差异（约 0.53 秒与 1.83 秒）。这说明该本机环境会受到 page cache、CPU
调度与瞬时批次时序影响；正确做法是增加运行轮数、预热/冷缓存分别测试，并记录分位数，**不能挑一次较好结果
当作系统能力**。但二者共同指出：上传尾延迟比下载更敏感，且并发 8 已明显进入排队区。

## 5. 过载保护验证

为了验证上限是否保护了服务，使用默认 DataNode 2 上传槽位，但让 benchmark 以 `concurrency=4`、
`global-chunk-budget=4` 直接超额提交 20 个上传样本。

```text
结果：2/20 成功；18/20 被 Gateway 以 HTTP 503
      "no DataNode write capacity available" 显式拒绝。
```

这不是一个“性能失败”：它表明 admission control 生效，系统没有为了接受所有请求而无界占用内存、block pool 或
磁盘队列。真正的产品缺口是 benchmark/客户端尚未实现收到可重试 `503` 后的指数退避、抖动和重试，而不是应当
删除这个保护。

## 6. 容量放大方向性实验

下表不是严格的单变量基准：它同时把 DataNode 的 `MAX_ACTIVE_UPLOADS` / `MAX_UPLOADS_PER_CLIENT` 与客户端
`global-chunk-budget` 从 2 调为 4。因此它只能证明调参方向，不足以说明每项改动各自贡献多少。

| 场景 | 同时上传 / 下载 | DataNode upload slots | client global budget | 上传 median / P95 / P99 (ms) | 下载 median / P95 / P99 (ms) | 上传/下载成功 |
|---|---:|---:|---:|---:|---:|---:|
| M8，默认重复基线 | 4 / 4 | 2 | 2 | 406.9 / 533.6 / 534.3 | 244.8 / 304.2 / 307.1 | 20 / 20 |
| M8-4，容量配置组 | 4 / 4 | 4 | 4 | 285.9 / 332.1 / 376.3 | 248.2 / 301.6 / 317.6 | 20 / 20 |

容量组的每轮总聚合吞吐约为 `234.8–283.0 MiB/s`。上传 P95 降低且没有错误，说明应继续执行严格的容量扫描；但它
不是把默认线上值立即修改为 4 的依据，因为三机环境、真实磁盘、每节点资源以及下载共存会改变最佳点。

### 6.1 4/8/12/16 上传槽位容量扫描

随后使用全新隔离集群完成了上传槽位扫描。每个档位同时调整 DataNode 的
`MAX_ACTIVE_UPLOADS`、`MAX_UPLOADS_PER_CLIENT` 与 benchmark 的 `global-chunk-budget`；磁盘执行器固定为
`2 worker + 128 block`。`chunk-window=2`，所以 `N` 个复制 Chunk 槽位对应 `N/2` 个完整文件同时推进。

```text
拓扑：1 Gateway + 2 DataNode，RF=2，loopback
对象：16 MiB = 4 × 4 MiB Chunk
轮数：每档 3 轮
```

| DataNode 上传槽位 | 同时上传文件 | 请求样本 | 完整成功 | 上传 median / P95 / P99 (ms) | 每轮聚合吞吐均值 (MiB/s) |
|---:|---:|---:|---:|---:|---:|
| 4 | 2 | 6 | 6/6 | 143.7 / 163.3 / 163.3 | 126.7 |
| 8 | 4 | 12 | 12/12 | 235.7 / 279.2 / 279.2 | 181.6 |
| 12 | 6 | 18 | 18/18 | 324.3 / 405.9 / 405.9 | 192.0 |
| 16 | 8 | 24 | 24/24 | 477.1 / 493.2 / 494.2 | 194.2 |

这里延迟随并发提高是正常现象：更多文件共享同一 CPU、内存、loopback 与 ext4 设备。需要关注的是聚合吞吐：

```text
4 → 8 槽：  126.7 → 181.6 MiB/s，约 +43%
8 → 12 槽： 181.6 → 192.0 MiB/s，约 +6%
12 → 16 槽：192.0 → 194.2 MiB/s，约 +1%
```

所以本机 loopback 环境的吞吐拐点约在 **8 个复制 Chunk 槽位**。16 槽确实把即时文件上传容量提高到 8 个，且
本轮没有 503 或校验错误，但它没有带来等比例吞吐提升。当前合理决策是：

```text
开发机/下一轮候选：maxActiveUploads=8，maxUploadsPerClient=8
客户端：chunk-window=2，global-chunk-budget=8
保守回退值：4
16：保留为容量实验值，不作为当前默认值
```

这仍不是三台真实服务器的最终配置；跨机 RF=2 会加入两次 NIC/Tailscale 路径、真实磁盘差异和 RTT，拐点可能前移。

### 6.2 16 槽最高档混合验证

对 16 槽配置又使用全新集群执行 3 轮 `8 上传 + 8 下载`，正式计时前等待 fixture 报告稳定 3 秒：

| 同时上传 / 下载 | 上传/下载样本 | 上传 median / P95 / P99 (ms) | 下载 median / P95 / P99 (ms) | 总成功 | 每轮聚合吞吐范围 |
|---:|---:|---:|---:|---:|---:|
| 8 / 8 | 24 / 24 | 651.9 / 715.5 / 723.8 | 558.4 / 613.5 / 621.1 | 48/48 | 268.7–290.3 MiB/s |

这证明当前实现能在该本机配置下完成 16 个文件级混合客户端，而不是只能做纯上传；但 P95 已达到约
`0.62–0.72 s`，并且纯上传吞吐在 8 槽后已经平台化。因此“16 槽可运行”不等于“16 槽是最佳默认值”。

### 6.3 固定 8 槽的完整上传矩阵

以下各档使用全新的隔离集群，避免较低并发测试累计写入、页缓存和磁盘状态污染后面的高并发档位。每个文件为
16 MiB、RF=2、4 × 4 MiB Chunk，`chunk-window=2`、`global-chunk-budget=8`，每档 5 轮。

| 同时上传文件 | 样本 | 成功 | median / P95 / P99 (ms) | 每轮聚合吞吐均值 (MiB/s) |
|---:|---:|---:|---:|---:|
| 1 | 5 | 5/5 | 132.3 / 171.6 / 171.6 | 64.6 |
| 2 | 10 | 10/10 | 147.5 / 194.7 / 194.7 | 122.2 |
| 4 | 20 | 20/20 | 220.0 / 257.7 / 268.4 | 186.9 |
| 6 | 30 | 30/30 | 288.7 / 378.0 / 397.7 | 190.0 |
| 8 | 40 | 40/40 | 414.1 / 503.5 / 550.2 | 196.6 |

`c6/c8` 并不表示 12/16 个 Chunk 同时进入服务端：8-Chunk 客户端预算会让额外文件在客户端排队。它测量的是
“同时提交更多文件后，有限预算能否稳定完成，以及文件级延迟如何增长”。结果显示：

```text
c4 → c6：聚合吞吐只增加约 1.7%，P95 增加约 47%
c4 → c8：聚合吞吐只增加约 5.2%，P95 增加约 95%
```

因此，8 个服务端复制 Chunk 槽位的合理前端默认值是 **4 个上传文件 × window 2**。允许 6/8 文件排队可作为
批量导入模式，但不应被称为更高效的默认配置。

### 6.4 固定 8 槽的完整下载矩阵

每个下载 fixture 在计时前上传完成并等待 1 秒，正式下载仍为 16 MiB 文件，每档使用全新集群并运行 5 轮。

| 同时下载文件 | 样本 | 成功 | median / P95 / P99 (ms) | 每轮聚合吞吐均值 (MiB/s) |
|---:|---:|---:|---:|---:|
| 1 | 5 | 5/5 | 149.3 / 179.3 / 179.3 | 103.4 |
| 2 | 10 | 10/10 | 157.8 / 188.4 / 188.4 | 188.8 |
| 4 | 20 | 20/20 | 205.8 / 271.1 / 275.8 | 299.9 |
| 8 | 40 | 40/40 | 258.4 / 307.4 / 318.5 | 449.9 |
| 16 | 80 | 80/80 | 488.7 / 560.1 / 587.7 | 473.3 |

两个 DataNode 各有 8 个下载槽，因此 c16 全部成功；但 `c8 → c16` 只增加约 5.2% 聚合吞吐，P95 却增加约
82%。在本机环境中，**8 个下载文件**是吞吐与长尾之间更合理的默认工作点；16 只作为突发容量上限。

### 6.5 固定 8 槽的混合负载矩阵

`mixed c=N` 中一半为上传、一半为下载。每档使用全新集群、5 轮、16 MiB 文件、1 秒 fixture settle。

| 同时上传 / 下载 | 上传/下载样本 | 总成功 | 上传 median / P95 / P99 (ms) | 下载 median / P95 / P99 (ms) | 每轮聚合吞吐均值 (MiB/s) |
|---:|---:|---:|---:|---:|---:|
| 2 / 2 | 10 / 10 | 20/20 | 124.5 / 157.5 / 157.5 | 208.8 / 247.3 / 247.3 | 230.9 |
| 4 / 4 | 20 / 20 | 40/40 | 284.1 / 322.6 / 339.1 | 270.5 / 336.5 / 349.2 | 279.7 |
| 6 / 6 | 30 / 30 | 60/60 | 364.8 / 461.8 / 524.0 | 407.9 / 471.2 / 499.5 | 282.9 |
| 8 / 8 | 40 / 40 | 80/80 | 437.2 / 555.9 / 569.9 | 513.3 / 1255.0 / 1264.6 | 272.0 |

`4+4 → 6+6` 的聚合吞吐仅增加约 1.1%，而上传/下载 P95 分别增加约 43%/40%；到 `8+8` 时吞吐反而下降，
下载 P95 超过 1.2 秒。由此确定本机的常规混合工作点为 **4 上传 + 4 下载**，而不是把成功完成的 8+8 当成默认值。

### 6.6 对象大小与端到端正确性

`end-to-end` 模式包含 Session、路由、RF=2 上传、File Commit、manifest、Chunk 下载与最终 SHA-256 对比。

| 大小 | 并发 | 样本 | 成功 | 上传 median / P95 (ms) | 下载 median / P95 (ms) |
|---:|---:|---:|---:|---:|---:|
| 64 KiB | 1 | 3 | 3/3 | 6.7 / 24.4 | 2.8 / 3.0 |
| 4 MiB | 1 | 3 | 3/3 | 82.9 / 88.2 | 46.2 / 68.5 |
| 16 MiB | 1 | 3 | 3/3 | 133.2 / 158.7 | 142.1 / 148.2 |
| 64 MiB | 1 | 3 | 3/3 | 487.4 / 559.5 | 580.7 / 596.3 |
| 256 MiB | 1 | 3 | 3/3 | 1941.3 / 2009.5 | 2107.8 / 2226.4 |
| 64 KiB | 4 | 12 | 12/12 | 8.2 / 23.8 | 2.9 / 3.8 |
| 4 MiB | 4 | 12 | 12/12 | 50.8 / 99.5 | 51.6 / 98.4 |
| 16 MiB | 4 | 12 | 12/12 | 201.1 / 251.6 | 157.6 / 231.0 |
| 64 MiB | 4 | 12 | 12/12 | 902.0 / 972.6 | 631.4 / 741.1 |
| 256 MiB | 4 | 4 | 4/4 | 3674.9 / 3719.8 | 5075.2 / 5251.3 |

上述 67 个端到端对象全部通过 SHA-256。它证明 8 槽配置不仅能跑固定的 16 MiB 样本，也覆盖单 Chunk 小对象和
64-Chunk 大对象。256 MiB c4 只有 1 轮、4 个样本，只能作为大对象烟测，不能拿 P95 当稳定 SLA。

### 6.7 小对象与控制面基础性能

为了测量 64 KiB 对象，benchmark 新增了 `KiB` 大小解析支持。使用 c8、25 轮写入 200 个 64 KiB 对象：

```text
成功：200/200
上传 median / P95 / P99：7.5 / 13.8 / 24.8 ms
每轮聚合吞吐均值：43.9 MiB/s，约等于 703 object/s
```

随后对含 200 个对象的目录进行独立 `curl` HTTP 请求；`p16` 表示 16 个并行客户端，每个 curl 是独立连接，未利用
一个长期 Keep-Alive 会话。

| 接口/操作 | 请求 | 成功 | 并发 | QPS | P50 / P95 / P99 / max (ms) |
|---|---:|---:|---:|---:|---:|
| catalog（200 项） | 500 | 500 | 1 | 73.7 | 1.395 / 1.787 / 2.388 / 7.212 |
| catalog（200 项） | 1000 | 1000 | 16 | 742.2 | 0.871 / 2.417 / 4.678 / 8.559 |
| object manifest | 500 | 500 | 1 | 80.0 | 0.558 / 0.777 / 1.208 / 2.712 |
| object manifest | 1000 | 1000 | 16 | 783.1 | 0.326 / 1.017 / 2.809 / 6.453 |
| object info | 1000 | 1000 | 16 | 807.7 | 0.288 / 0.751 / 1.541 / 5.931 |
| admin nodes | 1000 | 1000 | 16 | 806.8 | 0.297 / 1.023 / 2.841 / 5.762 |
| directory create | 100 | 100 | 1 | 78.3 | 0.575 / 0.868 / 0.985 / 1.164 |
| directory delete | 100 | 100 | 1 | 64.9 | 2.925 / 3.523 / 5.073 / 5.083 |

对象删除返回 200，随后读取同一 objectId 返回 404。控制面微基准共 5200 个请求全部成功，但其数据量仅 200 个对象，
不能外推到百万对象 Catalog；后续必须增加分页接口和大目录基准。

### 6.8 当前机器硬件上限与持续写入拐点

为了区分“代码平台化”和“硬件平台化”，在同一台 `ubuntu22data1` 虚拟机补充了 CPU、TCP、直接 I/O 与持续应用负载。
机器为 8 vCPU（4 core / 8 thread）、15 GiB 内存；仓库与 `/tmp` 均位于 50 GiB ext4 虚拟盘（测试时设备名
`/dev/sdb2`）。另一块
500 GiB `/data` 盘存在，但当前用户没有创建测试文件的权限，因此本轮没有伪造其结果。

约 5 GiB 临时副本数据已在测试后删除；保留下来的 `runs.csv`、summary、`vmstat`、`diskstats` 与 DataNode 日志位于：

```text
/tmp/minidriver-hw-evidence-0825/
```

#### 6.8.1 独立硬件能力

直接 I/O 使用非零 benchmark 数据，避免 `/dev/zero` 在虚拟磁盘或稀疏存储中被特殊优化。64 KiB 与当前
`SharedBodyBlock`/磁盘任务大小一致；1 MiB 表示更大顺序请求的参考上限。

| 能力 | 工作负载 | 实测上限 |
|---|---|---:|
| SHA-256 单进程 | OpenSSL，16 KiB block | 约 304.8 MiB/s |
| SHA-256 2 进程 | OpenSSL，16 KiB block | 约 624.2 MiB/s aggregate |
| SHA-256 4 进程 | OpenSSL，16 KiB block | 约 1255.7 MiB/s aggregate |
| loopback TCP 单流 | iperf3，5 秒 | 约 22.5 Gbit/s receiver |
| loopback TCP 四流 | iperf3，5 秒 | 约 21.4 Gbit/s receiver |
| 单流直接写 | 非零数据，64 KiB request，1 GiB | 约 152.2 MiB/s |
| 单流直接读 | 非零数据，64 KiB request，1 GiB | 约 194.7 MiB/s |
| 单流直接写 | 非零数据，1 MiB request，1 GiB | 约 214.2 MiB/s |
| 单流直接读 | 非零数据，1 MiB request，1 GiB | 约 222.6 MiB/s |
| 双流直接写 | 两路非零数据，各 512 MiB，64 KiB request | 约 117.0 MiB/s aggregate |
| 双流直接读 | 两路非零数据，各 512 MiB，64 KiB request | 约 106.7 MiB/s aggregate |

结论非常明确：loopback TCP 比应用快一个数量级以上，全机 SHA 吞吐也高于当前上传；64 KiB 双流直接写却只有约
`117 MiB/s`。本机 RF=2 的两个 DataNode 共用该盘，每个逻辑字节需要写两次，所以不考虑协议开销时的持续逻辑写入
上限约为：

```text
117.0 MiB/s physical aggregate ÷ RF=2 ≈ 58.5 MiB/s logical
```

这不是 MiniDriver 在三台独立磁盘服务器上的上限，只是当前“两个 DataNode 同盘”拓扑的硬件上限。分到两个独立磁盘
后，两份副本可并行，届时上限将由较慢 DataNode 的单盘写入、真实 NIC/Tailscale 和副本 ACK 共同决定。

#### 6.8.2 2.5 GiB 持续应用上传

为避免 16 MiB 短测只测到页缓存，使用推荐 8 槽配置执行：

```text
对象：64 MiB = 16 × 4 MiB Chunk
并发：4 个上传文件，window=2，global budget=8
轮数：10；共 40 个对象、2.5 GiB 逻辑数据、约 5 GiB RF=2 副本写入
结果：40/40 成功
```

全体文件延迟为：

```text
median / P95 / P99 / mean = 985.1 / 3301.3 / 3338.8 / 1763.1 ms
```

前 5 轮的 benchmark 聚合吞吐为 `183.2–191.8 MiB/s`，第 6 轮降到 `160.3 MiB/s`，第 7 轮骤降到
`60.9 MiB/s`。第 7～10 轮的单文件速率主要落在约 `19–25 MiB/s`，四文件完成时间推导的聚合上界约
`76.7–100.8 MiB/s`；它与上述双流直接写的 RF=2 逻辑硬件上限处于同一量级。

下降点并非随机。主机配置为：

```text
MemTotal ≈ 15.6 GiB
vm.dirty_background_ratio = 10
vm.dirty_ratio = 20
20% 脏页阈值约为 3 GiB
每轮物理副本写入 = 4 × 64 MiB × RF=2 = 512 MiB
6 轮物理副本写入 ≈ 3 GiB
```

因此第 7 轮正好跨过约 3 GiB 脏页阈值。`vmstat` 在持续阶段观察到平均约 `17.5% iowait`、平均 2.22 个阻塞任务，
高压样本出现 3～6 个阻塞任务与最高约 50% iowait；CPU 仍有明显 idle，不能解释为全机 CPU 已经耗尽。

两个 DataNode 的最终资源快照进一步确认了回写节流向应用背压传播：

| 指标 | Node-A | Node-C |
|---|---:|---:|
| BlockPool 峰值 | 8/8 MiB | 8/8 MiB |
| 最低可用 Block | 0（运行中观察） | 0 |
| Disk queue peak | 8 tasks | 8 tasks |
| 平均 queue wait | 约 1.22 ms/task | 约 1.17 ms/task |
| 最大 queue wait | 202.3 ms | 330.7 ms |
| 最大 work time | 201.6 ms | 203.0 ms |

测试结束立即读取 `/proc/diskstats` 时，`sdb2` 已写出约 4344 MiB，仍少于约 5120 MiB 的应用副本数据，说明
File Commit 成功时仍有一部分数据只完成了 `pwrite`、尚未全部从页缓存落到设备。这与当前无 `fsync/fdatasync` 的实现
边界一致。

#### 6.8.3 本机应使用的两个性能数字

```text
突发/热缓存上传：约 185–195 MiB/s logical，可维持到数 GiB 脏页预算耗尽。
同盘 RF=2 持续上传：约 60 MiB/s logical 量级，且尚未包含每对象强制持久化。
热缓存并发下载：报告已测约 450 MiB/s logical。
64 KiB 双流冷直接读：约 106.7 MiB/s aggregate，代表同盘冷读参考值。
```

所以后续优化不能再以“把 200 MiB/s 提到更高”为唯一目标。首先要把 burst、sustained、cold-cache 和 durable 四类
指标分开；否则增加 Worker、Block 或并发数只会扩大脏页和排队，掩盖真实磁盘能力。

#### 6.8.4 新增 100 GiB SSD 虚拟盘单流写入基线

新增 VHDX 经 Hyper-V SCSI 控制器接入后，在 Linux 中识别为 `/dev/sdc1`，使用 ext4 挂载到 `/data-ssd`，测试前约
92.9 GiB 可用。新增设备后 Linux 重新枚举了旧磁盘：当前 50 GiB 系统盘为 `/dev/sda2`、500 GiB 数据盘为
`/dev/sdb1`；部署配置应使用 filesystem UUID 或挂载点，不应长期依赖 `sda/sdb/sdc` 名称。

测试使用 1 GiB 非零 AES-CTR 数据，源文件预先刷盘并重新读入页缓存；目标使用 `O_DIRECT` 加 `fdatasync`，排除 Linux
目标页缓存突发速度。首次写入用于观察 VHDX/ext4 空间分配，随后覆盖同一已分配文件各 3 轮：

| 请求大小 | 场景 | 耗时 | 吞吐 |
|---:|---|---:|---:|
| 64 KiB | 首次分配 | 6.41 s | 约 159.8 MiB/s |
| 64 KiB | 覆盖 run 1 | 6.31 s | 约 162.3 MiB/s |
| 64 KiB | 覆盖 run 2 | 6.21 s | 约 164.9 MiB/s |
| 64 KiB | 覆盖 run 3 | 6.30 s | 约 162.5 MiB/s |
| 1 MiB | 覆盖 run 1 | 2.94 s | 约 348.3 MiB/s |
| 1 MiB | 覆盖 run 2 | 2.92 s | 约 350.7 MiB/s |
| 1 MiB | 覆盖 run 3 | 2.88 s | 约 355.6 MiB/s |

三轮稳定值折算为：

```text
64 KiB direct + fdatasync：约 163.2 MiB/s
1 MiB direct + fdatasync：约 351.5 MiB/s
```

目标文件 SHA-256 与源数据前 1 GiB 完全一致。首次分配与 64 KiB 覆盖写差距很小，说明本轮 1 GiB 范围内没有观察到
明显的动态 VHDX 首次扩展长尾。相比旧系统盘约 `152.2 MiB/s` 的 64 KiB 单流直接写，新盘在当前 MiniDriver 工作项
大小下提高约 7%；但 1 MiB 请求比 64 KiB 高约 2.15 倍，说明当前 64 KiB 网络 Block 不必修改，而磁盘执行器后续可以
测试将多个相邻 Block 合并成 256 KiB/1 MiB `pwritev`，以减少系统调用和虚拟存储栈开销。

这仍只是单流设备基线，不代表 RF=2 应用吞吐。下一步应让一个 DataNode 使用 `/data-ssd`、另一个使用独立旧盘，完成
MiniDriver 持续上传；不能把两个 DataNode 再放回同一个 SSD 后声称已经消除了副本磁盘竞争。

#### 6.8.5 fio 旧系统盘与新 SSD 的队列深度对比

为补足单线程 `dd` 无法测量并发队列的问题，使用 `fio 3.28 + libaio` 对两个 ext4 文件系统执行完全相同的稳态矩阵。
测试只写独立临时文件，不直接写裸设备：

```text
旧盘：/dev/sda2，测试目录 /tmp
新盘：/dev/sdc1，测试目录 /data-ssd
文件：每盘一个已预写/已分配的 2 GiB 文件
公共参数：direct=1，numjobs=1，time_based=1，runtime=15s，ramp_time=3s
写测试：end_fsync=1
```

| 工作负载 | 旧盘吞吐 | 新盘吞吐 | 新/旧 | 旧盘 P95 clat | 新盘 P95 clat |
|---|---:|---:|---:|---:|---:|
| 64 KiB，QD1，write | 84.33 MiB/s | 202.32 MiB/s | 2.40× | 0.725 ms | 0.403 ms |
| 64 KiB，QD8，write | 111.06 MiB/s | 487.73 MiB/s | 4.39× | 20.58 ms | 1.057 ms |
| 1 MiB，QD32，write | 144.48 MiB/s | 501.43 MiB/s | 3.47× | 935.33 ms | 70.78 ms |
| 1 MiB，QD32，read | 125.39 MiB/s | 536.49 MiB/s | 4.28× | 413.14 ms | 62.65 ms |

64 KiB 写 IOPS 对比：

```text
旧盘 QD1 / QD8：1349 / 1776 IOPS
新盘 QD1 / QD8：3237 / 7803 IOPS
```

所有正式档位的 fio 设备利用率都约为 `92–99.5%`，而 fio 进程 CPU 未成为上限。旧盘 QD1→QD8 吞吐仅增加约 32%，
P95 却从 0.725 ms 恶化到 20.58 ms；新盘吞吐增加约 141%，P95 只升到约 1.06 ms。这证明新盘真正的优势不是 QD1
单流多出的几十 MiB/s，而是可以有效消化并发队列，旧盘则在队列加深后迅速形成长尾。

本轮测到的新盘最高档约为 `501 MiB/s` 写、`536 MiB/s` 读。它是当前 VHDX、ext4、libaio、单 job/QD32 配置下的
已验证饱和点，不是宿主物理 SSD 的裸设备绝对上限；但设备利用率接近 100%，继续单纯增加 QD 的收益预计有限，后续
若要验证应增加 QD16/64 和 numjobs=2/4，而不能直接宣称厂商级峰值。

对 MiniDriver 的直接含义是：当前 64 KiB 网络 Block 可以保留，但同步 `pwrite` 加 2 个 Disk Worker 只能形成很浅的
设备队列，可能无法利用新盘 QD8 的约 `488 MiB/s` 能力。后续应先在新盘上扫描 worker=2/4/8，再测试相邻 Block 合并
或 `pwritev`；对旧盘盲目增加 Worker 只会放大 P95。若本机 RF=2 分别使用一块旧盘和一块新盘，持续吞吐仍受较慢的
旧盘约束，但会消除原来两个副本共同争抢同一系统盘的问题。

机器可复查的 fio JSON 证据保存在：

```text
/tmp/minidriver-fio-evidence-20260825/
```

#### 6.8.6 MiniDriver 整体迁移到新 SSD 后的同参数复测

本轮不是再次运行独立 fio，而是把此前 MiniDriver 基准的完整 I/O 路径迁入新盘：

```text
/data-ssd/minidriver-perf-newssd-20260825/<case>/cluster/gateway
/data-ssd/minidriver-perf-newssd-20260825/<case>/cluster/node-a/disk0.data
/data-ssd/minidriver-perf-newssd-20260825/<case>/cluster/node-c/disk0.data
/data-ssd/minidriver-perf-newssd-20260825/<case>/work
```

公共条件保持为单机 loopback、`1 Gateway + 2 DataNode`、RF=2、4 MiB Chunk、2 个 Disk Worker、128 个 64 KiB
Block、8 个上传/下载槽位。除专门的 slot scan 外，客户端为 `chunk-window=2`、`global-chunk-budget=8`。每个并发档
使用全新的隔离集群；测试后删除 DataNode、Gateway 和 benchmark 临时文件，只保留 CSV、JSON、console、vmstat、
diskstats 与日志证据。

本轮产生 22 份 `summary.csv`，覆盖上传、下载、混合、槽位、持续写、小对象和端到端对象大小矩阵；合计
`815/815` 条数据面记录成功。原报告的 5200 请求控制面微基准没有重复执行，因为本轮变量是 DataNode Body 所在磁盘；
其旧盘结果继续保留在 6.7，不能冒充新盘控制面结果。

这里仍有两个必须保留的真实性边界：

1. 两个 DataNode 仍共享同一个 `/dev/sdc1`，所以这不是两台独立 SSD 服务器；它只是把旧的“两个副本同系统盘”
   替换成“两个副本同新 SSD”。
2. `FastDataStore` 当前以普通 `pwrite` 写入，文件未使用 `O_DIRECT/O_DSYNC`，Chunk 完成没有 `fdatasync/fsync`；
   LevelDB 也使用默认非同步 `WriteOptions`。因此短时 File Commit 测的是应用确认和页缓存接纳，不是断电持久化确认。

##### 上传槽位扫描

| 每 DataNode 上传槽位 | 同时上传文件 | 旧盘聚合吞吐 | 新盘聚合吞吐 | 新盘成功 | 新盘 median / P95 (ms) |
|---:|---:|---:|---:|---:|---:|
| 4 | 2 | 126.7 MiB/s | 112.3 MiB/s | 6/6 | 156.4 / 181.9 |
| 8 | 4 | 181.6 MiB/s | 175.6 MiB/s | 20/20 | 236.1 / 262.5 |
| 12 | 6 | 192.0 MiB/s | 186.4 MiB/s | 18/18 | 348.3 / 381.8 |
| 16 | 8 | 194.2 MiB/s | 195.1 MiB/s | 24/24 | 461.2 / 526.1 |

新盘并没有把 16 MiB 短时上传平台抬到 fio 的 `488 MiB/s`。12→16 槽只增加约 4.7%，文件 P95 增加约 38%；
因此 8 槽仍是推荐 admission 配置，16 槽只是已验证的突发容量，不应因为更换磁盘就成为默认值。

##### 固定 8 槽的 16 MiB 上传矩阵

| 同时上传 | 旧盘聚合吞吐 | 新盘聚合吞吐 | 新盘成功 | 新盘 median / P95 / P99 (ms) |
|---:|---:|---:|---:|---:|
| 1 | 64.6 MiB/s | 61.9 MiB/s | 5/5 | 155.4 / 157.6 / 157.6 |
| 2 | 122.2 MiB/s | 124.1 MiB/s | 10/10 | 145.9 / 173.9 / 173.9 |
| 4 | 186.9 MiB/s | 175.6 MiB/s | 20/20 | 236.1 / 262.5 / 270.5 |
| 6 | 190.0 MiB/s | 176.7 MiB/s | 30/30 | 310.5 / 367.6 / 370.6 |
| 8 | 196.6 MiB/s | 179.9 MiB/s | 40/40 | 420.4 / 534.0 / 599.1 |

新旧盘各档存在约 ±10% 的运行波动，但曲线形态完全相同：`c4` 后吞吐基本平台化，而文件级长尾继续增长。说明短时
上传首先受应用路径与有限 Chunk budget 约束；仅换盘不能解决该平台。

##### 固定 8 槽的 16 MiB 下载矩阵

| 同时下载 | 旧盘聚合吞吐 | 新盘聚合吞吐 | 新盘成功 | 新盘 median / P95 / P99 (ms) |
|---:|---:|---:|---:|---:|
| 1 | 103.4 MiB/s | 92.7 MiB/s | 5/5 | 175.1 / 209.4 / 209.4 |
| 2 | 188.8 MiB/s | 181.5 MiB/s | 10/10 | 171.0 / 191.5 / 191.5 |
| 4 | 299.9 MiB/s | 311.9 MiB/s | 20/20 | 176.2 / 226.1 / 232.1 |
| 8 | 449.9 MiB/s | 416.2 MiB/s | 40/40 | 280.9 / 312.1 / 327.7 |
| 16 | 473.3 MiB/s | 475.3 MiB/s | 80/80 | 486.7 / 565.2 / 578.7 |

16 MiB fixture 在正式计时前刚刚写入，读取很容易命中 guest/host page cache；因此该矩阵测的是热缓存端到端路径。
新旧盘的 c16 几乎相同，不能由此推导新 SSD 的冷盘读取能力。

##### 固定 8 槽的混合矩阵

| 同时上传 / 下载 | 旧盘聚合吞吐 | 新盘聚合吞吐 | 新盘成功 | 新盘上传 P95 | 新盘下载 P95 |
|---:|---:|---:|---:|---:|---:|
| 2 / 2 | 230.9 MiB/s | 232.5 MiB/s | 20/20 | 157.7 ms | 271.7 ms |
| 4 / 4 | 279.7 MiB/s | 279.4 MiB/s | 40/40 | 308.6 ms | 338.9 ms |
| 6 / 6 | 282.9 MiB/s | 278.0 MiB/s | 60/60 | 479.7 ms | 472.5 ms |
| 8 / 8 | 272.0 MiB/s | 272.5 MiB/s | 80/80 | 669.9 ms | 628.8 ms |

常规 `4 上传 + 4 下载` 的聚合吞吐只相差约 0.1%；`6+6` 后吞吐不再增长，长尾继续恶化。因此新盘复测不改变
原推荐工作点。

##### 5 GiB RF=2 持续回写

使用与旧盘完全相同的 `64 MiB × 4 并发 × 10 轮`，共写入 2.5 GiB 逻辑对象和约 5 GiB DataNode 副本：

| 指标 | 旧系统盘 | 新 SSD |
|---|---:|---:|
| 成功 | 40/40 | 40/40 |
| 文件 median | 985.1 ms | 976.3 ms |
| 文件 P95 | 3301.3 ms | 1115.6 ms |
| 文件 P99 | 3338.8 ms | 1168.8 ms |
| 文件 mean | 1763.1 ms | 985.2 ms |
| 聚合吞吐范围 | 约 60.9–191.8 MiB/s | 158.1–185.7 MiB/s |
| `vmstat` 平均 iowait | 约 17.5% | 约 3.0% |
| `vmstat` 平均 blocked task | 约 2.22 | 约 0.56 |

新盘 10 轮聚合吞吐依次为：

```text
177.3 / 184.0 / 173.9 / 177.3 / 180.3 /
184.5 / 170.9 / 158.1 / 183.5 / 185.7 MiB/s
```

旧盘在第 7 轮跨过脏页阈值后出现约 `61 MiB/s` 的断崖；新盘没有重复该现象。这证明 fio 的优势确实能转化为
MiniDriver 的持续回写稳定性和 P95，而不是转化为更高的短时平台。新盘测试期间 `/dev/sdc` 的 block-device 写入增量
约为 4966 MiB，但该数字同时包含 benchmark 输入文件和内核异步回写，不能简单等同于“5 GiB 副本全部持久化”。

##### 对象大小与端到端 SHA-256

| 大小 | 并发 | 成功 | 新盘上传 median / P95 | 新盘下载 median / P95 |
|---:|---:|---:|---:|---:|
| 64 KiB | 1 | 3/3 | 6.3 / 19.6 ms | 1.7 / 3.0 ms |
| 4 MiB | 1 | 3/3 | 99.7 / 104.0 ms | 49.3 / 49.7 ms |
| 16 MiB | 1 | 3/3 | 133.0 / 133.5 ms | 174.1 / 202.9 ms |
| 64 MiB | 1 | 3/3 | 605.1 / 616.9 ms | 614.7 / 638.4 ms |
| 256 MiB | 1 | 3/3 | 2103.1 / 2124.1 ms | 2297.6 / 2313.9 ms |
| 64 KiB | 4 | 12/12 | 7.7 / 15.7 ms | 3.3 / 6.9 ms |
| 4 MiB | 4 | 12/12 | 58.5 / 86.7 ms | 48.5 / 60.9 ms |
| 16 MiB | 4 | 12/12 | 215.0 / 264.1 ms | 160.2 / 224.6 ms |
| 64 MiB | 4 | 12/12 | 914.9 / 976.1 ms | 628.3 / 693.8 ms |
| 256 MiB | 4 | 4/4 | 3989.3 / 4030.3 ms | 2463.5 / 2557.0 ms |

全部 67 个对象通过最终 SHA-256。多数短时档位与旧盘相近；最明显改善是 256 MiB c4 下载 median 从约
`5075 ms` 降到 `2463 ms`，说明更快的并发 I/O 对大对象更有价值。该档仍只有 4 个样本，不能将 P95 写成稳定 SLA。

##### 小对象

`64 KiB × c8 × 25 轮` 共 200 个对象全部成功：

```text
median / P95 / P99 = 8.4 / 15.7 / 21.8 ms
聚合吞吐均值       = 41.6 MiB/s，约 666 object/s
```

旧盘为约 43.9 MiB/s、703 object/s；差异不大，说明当前小对象更受 Session/Route/HTTP/LevelDB 固定开销影响。

综合判断：新 SSD 已解决本机同盘 RF=2 的持续回写断崖，并显著改善大对象并发读取；它没有解决约 180–200 MiB/s 的
短时上传平台。下一轮性能优化必须将“页缓存突发”和“真正 durable commit”分开：先定义 `fdatasync`/group commit
策略和独立 benchmark client 盘，再在新 SSD 上扫描 Disk Worker 2/4/8 与相邻 Block 合并，否则只提高槽位会继续增加
文件长尾。

机器可复查证据位于：

```text
/tmp/minidriver-perf-newssd-evidence-20260825/
```

#### 6.8.7 同一新 SSD：MinIO/Warp 与 MiniDriver 单 DataNode 对照

为了回答“这块盘上的成熟对象存储能跑多少，以及 MiniDriver 去掉 RF=2 后自身能跑多少”，本轮增加了两套完全隔离的
单机基线。所有服务端数据均位于 `/data-ssd`，客户端工作目录放在 `/tmp`，避免 benchmark 输入文件与服务端竞争同一
块新 SSD；测试结束后已经停止容器并删除服务端数据，只保留 `/tmp` 下的结果与日志。

```text
MinIO：官方 quay.io/minio/minio:latest
       RELEASE.2025-09-07T16-13-09Z，单进程、单目录、无纠删码副本
Client：官方 minio/warp 1.3.1

MiniDriver：1 Gateway + 1 DataNode，4 MiB Chunk
            2 个 Disk Worker、128 × 64 KiB Block
            上传/下载 admission 上限 64；每个上传 window=2

共同条件：/dev/sdc1 ext4、loopback、16 MiB 对象；不是跨机网络测试
```

为支持这条可复跑基线，`tools/start_v2_local_benchmark_cluster.sh` 的节点数校验已从 `2–4` 扩展为 `1–4`。单 DataNode
只用于测量单副本数据路径；当前文件目标 RF 仍是 2，因而对象可能保持 `PROTECTING`，不能作为生产拓扑或高可靠验收。

##### 16 MiB 纯上传应用确认吞吐扫描

MinIO 每档使用 Warp 持续运行约 15 秒；MiniDriver 每档 5 轮。两种 client、请求协议和计时方式不同，绝对延迟不能视为
逐请求等价，但吞吐曲线足以标定同机数量级与并发拐点。

| 并发 | MiniDriver 吞吐 | MiniDriver median / P99 | MinIO PUT 吞吐 | MinIO P50 / P99 |
|---:|---:|---:|---:|---:|
| 1 | 66.97 MiB/s | 127.4 / 141.4 ms | 75.39 MiB/s | 212.9 / 291.2 ms |
| 2 | 129.71 MiB/s | 128.8 / 145.9 ms | 121.69 MiB/s | 267.2 / 295.8 ms |
| 4 | 203.95 MiB/s | 180.2 / 216.3 ms | 169.18 MiB/s | 377.5 / 414.6 ms |
| 8 | 211.10 MiB/s | 399.2 / 458.6 ms | 213.11 MiB/s | 626.2 / 738.0 ms |
| 16 | **234.75 MiB/s** | 680.2 / 810.5 ms | **275.43 MiB/s** | 966.7 / 1443.3 ms |
| 32 | 221.73 MiB/s | 1464.9 / 1959.8 ms | 265.09 MiB/s | 2073.6 / 3153.3 ms |
| 64 | 未测 | - | 224.66 MiB/s | 4632.4 / 5546.4 ms |
| 128 | 未测 | - | 212.17 MiB/s | 9099.8 / 11612.2 ms |

两条曲线的峰值都在 c16，之后吞吐下降而 P99 快速增长。MinIO c16 比 MiniDriver 单副本约高 `17.3%`；相对新 SSD
`fio 64 KiB/QD8 = 487.73 MiB/s` 的直接写基线，二者逻辑吞吐约为 56.5% 与 48.1%。这个比例只用于定位应用路径仍有
优化空间：fio 使用 direct I/O，两个对象存储使用页缓存、metadata 和不同 client，不能把比例解释为严格设备效率。

MinIO 高并发档的 `/dev/sdc` block-device 写入增量约为 4.0–5.0 GiB，只能说明压测期间有脏页被内核异步提交到了
块设备。它不能证明每个 PUT 响应之前对应对象已经完成持久化，也不能证明数据与 metadata 的持久化顺序。本轮没有
跟踪 MinIO 内部每次 PUT 的同步点，也没有执行断电/崩溃恢复实验，因此 MinIO 数字同样只按应用确认吞吐使用；不能拿
它与 MiniDriver 做 durable throughput 排名。

##### 小对象、大对象与持续写

| 工作负载 | MiniDriver 单副本 | MinIO 单盘 | 直接结论 |
|---|---:|---:|---|
| 64 KiB PUT，c16 | 38.78 MiB/s，约 621 object/s；P50/P99 15.7/38.9 ms | 75.77 MiB/s，约 1212 object/s；P50/P99 12.3/35.0 ms | MinIO object/s 约 1.95×；MiniDriver 的 Session/Route/HTTP/LevelDB 固定成本最值得先优化 |
| 256 MiB PUT，c4 | 209.30 MiB/s；median/P99 3135.9/3255.2 ms | 252.01 MiB/s；P50/P99 3944.0/4329.9 ms | MinIO 吞吐约高 20.4%；协议 client 不同，不横向比较单请求延迟 |
| 64 MiB PUT，c4，10 轮 | 198.24 MiB/s；40/40，P95 1201.5 ms | 未做相同轮次模型 | 单副本持续吞吐比同盘 RF=2 的 177.54 MiB/s 高约 11.7% |

MiniDriver 单节点整轮共 `1064/1064` 个记录成功，没有 503、内容校验失败或进程崩溃。256 MiB 端到端 c4 另有
`4/4` 个对象完成上传、下载与最终 SHA-256 校验；纯 PUT 数据见上表，不能把端到端上传+下载聚合吞吐混入纯写比较。

##### 下载和混合负载为何不直接排名

MinIO Warp 的 16 MiB GET 在 c1/c4/c8/c16 分别达到约 `569/2731/3216/3240 MiB/s`，远高于 fio 的 536 MiB/s
读取上限。这明确说明 fixture 命中了 guest/host page cache，测到的是内存、HTTP 和 Warp 消费路径，不是 SSD 冷读。
MiniDriver 同样会命中页缓存，但 benchmark 还会逐 4 MiB Chunk 组装 HTTP 响应、写入客户端输出文件并做最终 SHA-256，
所以不能用两组热读数字宣称谁的磁盘读取更快。

混合负载也只保留为各自稳定性证据：MiniDriver 固定一半上传、一半下载，c4/c8/c16 分别约
`233.54/304.70/323.07 MiB/s`，全部 `140/140` 成功；Warp 使用概率型 50/50 GET/PUT，且 GET 命中热缓存。下一轮若要
做严格产品对比，必须统一对象集合、持续时间、client 是否落盘、校验方式、冷/热缓存与 durability 语义。

##### 本轮可以成立的判断

```text
新 SSD fio 64 KiB/QD8 direct write：       约 487.73 MiB/s
MinIO 单盘 16 MiB PUT 应用确认峰值：       约 275.43 MiB/s @ c16
MiniDriver 单副本 PUT 应用确认峰值：        约 234.75 MiB/s @ c16
MiniDriver 同盘 RF=2 非 durable 持续逻辑写：约 177.54 MiB/s
```

因此当前约 200 MiB/s 不是 SSD 的极限，也不是“使用 HTTP 就只能到这里”。成熟 MinIO 在相同硬件和 HTTP/S3 上的
本轮应用确认 PUT 约为 275 MiB/s；MiniDriver 单副本已达到相同数量级。下一步优先定位 CPU copy/SHA、64 KiB Block 到
`pwrite` 的提交方式、Session/Route/LevelDB 固定开销与 RF=2 链式复制阶段，而不是先改成 RPC、HDFS 或 Kubernetes。
但在任何性能优化之前，都应先定义 MiniDriver 的 durable commit 边界，否则优化后的高吞吐仍只代表“更快写入页缓存”。

机器可复查证据位于：

```text
/tmp/minidriver-minio-bench-evidence-20260825/
/tmp/minidriver-single-node-evidence-20260825/
```

#### 6.8.8 能否优化到 MinIO 档次，以及“控制面不足”具体指什么

答案需要按对象大小和可靠性语义分别判断：

```text
16 MiB 单副本：MiniDriver 约 235 MiB/s，MinIO 约 275 MiB/s，只差约 17%。
64 KiB 单副本：MiniDriver 约 621 object/s，MinIO 约 1212 object/s，差约 1.95×。
RF=2：MiniDriver 还要传输并写入第二份数据，不能与 MinIO 单盘单副本直接排名。
durable：本轮双方没有建立相同的响应前强制持久化条件，不能用现有数字设 durable 目标。
```

因此，大对象单副本优化到本轮 MinIO 的同一数量级是合理工程目标，但不能提前承诺一定超过。17% 差距可能来自可修正
的 client、控制往返、copy/SHA 和写入提交方式，也包含两套 benchmark 不完全等价的测量误差。小对象要接近 MinIO，
则必须专门治理控制面和连接复用；只增加 Disk Worker 基本不会消除约 2 倍的 object/s 差距。

##### 控制面和数据面分别是什么

```text
控制面：Session、对象身份、Chunk manifest、Placement、Lease、Capability、
        Chunk/File Commit、目录/Catalog、Outbox、节点健康。

数据面：客户端或副本节点传输文件 Body，DataNode 做 SHA-256、pwrite、
        副本链传输和读取响应。
```

Gateway 不传文件 Body 是正确边界；“控制面不足”不是说应该把这些规则删掉，而是目前完成同一业务动作需要太多固定
往返，并且关键状态操作过度串行化。

这里还要区分控制面的两类不足：

| 维度 | 当前不足 | 应由什么阶段解决 |
|---|---|---|
| 单机并发性能 | 单 Gateway 全局锁、逐 Chunk 控制往返、LevelDB 写放大、连接复用不足 | 当前性能治理阶段：批量、Keep-Alive、分段观测、缩小锁范围 |
| 分布式正确性/高可用 | 当前数据路径仍以单 Gateway 的内存和本地 LevelDB 为真相；Gateway 故障后没有三副本共识、Leader 切换、epoch/generation fencing | V3-Lite MetadataStateMachine + 3 Raft Member |
| 持久性 | DataNode 数据索引和 Gateway metadata 没有统一的 durable commit 顺序 | durable commit/group commit 设计，并纳入 V3 命令结果 |
| 横向扩展 | 多 Gateway 还不能无状态地共享同一 Metadata 真相 | V3-Lite 验证后再进入 V3.1 多 Gateway/LB |

所以 MinIO 对照只能回答“同一台机器的单副本应用路径还有多少优化空间”，不能证明 MiniDriver 已具备 MinIO 的完整
S3 兼容、故障恢复或运维成熟度。反过来，Raft 解决状态真相和故障切换，也不会自动提高单机吞吐；共识还会增加一次
多数派提交成本，必须通过小命令、批处理和不让文件 Body 进入 Raft 来控制开销。

##### 当前一次上传的控制放大

当前 benchmark 使用兼容的 Session 路径。一个文件至少执行：

```text
1 × POST Gateway create session
1 × GET  Gateway session
N × POST Gateway route（当前 client 每个 Chunk 单独请求）
N × PUT  DataNode Chunk Body
N × POST DataNode → Gateway internal chunk commit
1 × POST Gateway file commit
```

所以 64 KiB 单 Chunk 对象需要 6 次 HTTP 事务；16 MiB/4 Chunk 对象需要 15 次，其中 11 次属于 Gateway 控制路径或
内部回调。网页已经使用更完整的 Preflight，但仍然逐 Chunk 请求 Route；浏览器通常可以复用 HTTP/1.1 连接，而当前
benchmark client 明确发送 `Connection: close`，每次控制请求和 PUT 都重新建 TCP 连接。Warp 的连接策略不同，因此
现有小对象差距中同时包含真实服务端开销和 benchmark client 开销，必须先拆开测量。

此外，benchmark 在计时区间内会先读取并计算每个 Chunk SHA-256，再把数据重新读取并发送；MinIO Warp 不遵循完全
相同的客户端哈希流程。这也是不能把 17% 全部归咎于 MiniDriver 服务端的原因。

##### 已由代码确认的控制面瓶颈候选

| 候选 | 当前实现 | 为什么会限制并发 |
|---|---|---|
| Gateway 全局锁 | Session、Route、Chunk Commit、File Commit 等均使用同一个 `GatewayState::mutex_` | 不同文件的控制操作也串行；LevelDB 调用和部分扫描发生在持锁区间 |
| 每 Chunk 两次 metadata 写 | `commitChunk()` 依次 `persistRouteLocked()` 和 `persistSessionLocked()` | 每块至少两个 LevelDB Write，且在全局锁内；小对象尤其受固定开销影响 |
| Route 请求粒度 | API 能接收数组，但 benchmark 和网页当前每次只提交一个 Chunk | 增加 JSON、HMAC Capability、TCP/HTTP 和 EventLoop 调度次数 |
| Session 冗余读取 | benchmark 的 create 响应已有 `chunkSize/totalChunks`，随后仍 GET Session | 每文件多一次控制请求；正常新上传并不需要这次读取 |
| 连接复用不足 | benchmark 所有 HTTP 请求使用 `Connection: close` | 小对象会把 TCP 建连/关闭成本放大；不能代表服务端自身极限 |
| 缓存失效粒度 | Commit 后存在 `catalogCache_.clear()`、`manifestCache_.clear()` | 数据规模和写并发升高后，全量失效会增加读重建与锁竞争 |
| 持久化协议缺失 | 数据和 LevelDB 都没有响应前同步及顺序约束 | 当前速度较高但不是 durable；以后直接逐 Chunk fsync 又可能造成同步风暴 |

DataNode 也有独立的数据面候选：网络以 64 KiB Block 流入时，每个 Block 分别执行 SHA update 和阻塞 `pwrite`；4 MiB
Chunk 通常对应约 64 次写调用。当前只有 2 个 Disk Worker，尚未比较相邻 Block 合并为 256 KiB/1 MiB、`pwritev`
或更深异步 I/O 队列后的结果。RF=2 还会增加一条 DN→DN 传输、第二份写入和副本完成等待。

##### 哪些是必要成本，不能为了跑分快速删除

```text
对象版本和 manifest 校验：保证对象身份与内容一致。
Placement / Lease / Capability：防止客户端绕过路由及过期写入。
Chunk Commit / File Commit：防止文件在 Chunk 未完成时变为可见。
RF=2 与 SHA-256：提供副本保护和完整性，不能拿单副本成绩替换。
Outbox：保证对象 Commit 后索引事件最终可发布。
```

优化原则应该是批量、复用、减少锁范围和减少重复计算，而不是移除正确性边界。HTTP/1.1 本身也不是首要问题；MinIO
同样通过 HTTP/S3 达到约 275 MiB/s，说明先改 RPC 不会自动解决全局锁、metadata 放大或 64 KiB `pwrite` 问题。

##### 建议的四步优化顺序

```text
P0 测量拆分
  benchmark 增加 session/create、client hash、route、PUT、chunk commit、file commit 分段耗时；
  Gateway 增加 mutex wait/hold、LevelDB read/write count/latency；
  增加 data-plane-only 与 control-plane-only 两种基准。

P1 先消除不必要的控制放大
  benchmark/client 使用 keep-alive；新 Session 不再重复 GET；
  一次请求批量获得 window 内多个 Route；Chunk Commit 的 Route+Session 使用一个 WriteBatch；
  与旧客户端保留 API 兼容。

P2 再优化数据管线
  固定 slots 后扫描 Disk Worker 2/4/8；合并相邻 Block；
  记录每字节 copy 次数、SHA 和 pwrite 时间；保持有界内存与背压。

P3 最后建立可靠性能线
  定义 data fdatasync、index sync 和 File metadata sync 的顺序；
  使用 group commit，而不是每个 64 KiB Block 单独 fsync；
  分别发布 burst、sustained、durable、RF=1、RF=2 五组结果。
```

第一阶段可以采用以下目标，但它们是验收门槛，不是尚未实现的性能声明：

| 模式 | 第一目标 | 说明 |
|---|---:|---|
| 16 MiB、RF=1、非 durable、c16 | ≥250 MiB/s 或达到同条件 MinIO 的 90% | 先缩小当前 17% 差距，同时限制 P99 |
| 64 KiB、RF=1、非 durable、c16 | ≥900 object/s | 先证明控制面治理有效，再评估是否追到 1200 object/s |
| RF=2、非 durable | 比当前持续 177.54 MiB/s 提升且不恶化长尾 | 必须报告逻辑吞吐和两份物理流量 |
| RF=2、durable | 实测后确定 | 不使用当前非 durable 数字推算或承诺 |

只有 P0 分段指标证明耗时集中在控制面后，才实施锁分片或 metadata 批处理；如果主要时间在 client hash、DataNode SHA、
copy 或 `pwrite`，就应优先优化对应数据路径。这样才不会为了追 MinIO 数字而重构错层。

#### 6.8.9 网络流式上限与磁盘请求大小解耦扫描（2026-08-26）

为了判断 `64 KiB` 到底是硬件选择还是代码选择，本轮先做两组不修改 MiniDriver 业务协议的底层扫描。它们回答的是
“本机底层曲线在哪里”，不是修改后的 MiniDriver 端到端成绩。

##### iperf3 loopback 应用缓冲大小

```text
工具：iperf3 3.9
地址：127.0.0.1
每档：1 秒 warm-up + 3 秒计时
变量：iperf3 -l 应用读写 buffer；P=1 / P=4
```

| `iperf3 -l` | 单流发送吞吐 | 4 流聚合发送吞吐 |
|---:|---:|---:|
| 4 KiB | 9.65 Gbit/s | 10.95 Gbit/s |
| 16 KiB | 17.05 Gbit/s | 19.85 Gbit/s |
| 32 KiB | 19.26 Gbit/s | 20.57 Gbit/s |
| 64 KiB | 20.10 Gbit/s | 22.35 Gbit/s |
| 128 KiB | 22.22 Gbit/s | 23.02 Gbit/s |
| 256 KiB | **22.75 Gbit/s** | **23.14 Gbit/s** |
| 1 MiB | 19.21 Gbit/s | 22.89 Gbit/s |

`64 KiB` 在单流下约达到本轮 256 KiB 峰值的 88%，在 4 流下约达到 97%；128–256 KiB 是本机 loopback 的平台区，
1 MiB 没有继续提高。这证明过小的 4/16 KiB 会放大调用成本，但不能证明 MiniDriver 应把网络 Block 直接改成 256 KiB：
当前 MiniDriver 上传约 200 MiB/s（约 1.6 Gbit/s），远低于 64 KiB loopback 的约 20 Gbit/s，而且真实三机还受 NIC、
Tailscale 和 RTT 约束。

这里的 `-l` 是 iperf3 每次应用读写的 buffer 大小，不是 TCP 包大小；内核仍会按 MSS、TSO/GSO、拥塞窗口和实际可读
字节切分。它只用于观察应用调用粒度，不代表线上每个 `recv()` 都会恰好收到对应字节数。

##### 新 SSD Direct I/O 请求大小

```text
设备：/dev/sdc1，ext4，Hyper-V 100 GiB 虚拟 SSD
工具：fio 3.28，libaio，direct=1，顺序写，1 GiB 非重复 buffer
每档：单 job，end_fsync=1，测试文件结束后自动 unlink
变量：bs=32 KiB～4 MiB；iodepth=1 / 8
```

| fio block size | QD1 顺序写 | QD8 顺序写 |
|---:|---:|---:|
| 32 KiB | 115 MiB/s | 472 MiB/s |
| 64 KiB | 180 MiB/s | 494 MiB/s |
| 128 KiB | 217 MiB/s | 495 MiB/s |
| 256 KiB | 254 MiB/s | 504 MiB/s |
| 512 KiB | 299 MiB/s | **505 MiB/s** |
| 1 MiB | 337 MiB/s | 500 MiB/s |
| 4 MiB | **382 MiB/s** | 500 MiB/s |

QD1 对请求大小非常敏感：64 KiB 只有约 180 MiB/s，增大到 256 KiB/1 MiB/4 MiB 后依次达到约
254/337/382 MiB/s。QD8 则从 64 KiB 起已接近设备平台，256–512 KiB 约为 504–505 MiB/s。由此可以把硬件与
代码责任拆开：

```text
硬件/虚拟化栈决定：不同 request size 和 queue depth 对应的吞吐/延迟曲线。
MiniDriver 框架决定：能否合并相邻 Block、形成多深的在途 I/O、何时背压、占用多少内存。
业务负载决定：小对象延迟、批量大对象吞吐、RF=2 与公平性之间如何取舍。
```

当前每条 `ChunkDiskWritePipeline` 使用 `appendInFlight_` 串行提交，一个 64 KiB Body Block 通常对应一个 SHA update、
一个 DiskExecutor Task 和一次同步 `pwrite`。多文件与两个 Disk Worker 可以产生一些设备并发，但代码没有显式形成、
控制或观测 QD8 的连续批量提交。因此既不能把 QD1/64 KiB 的 180 MiB/s 说成硬件极限，也不能直接把 QD8/fio 的
505 MiB/s 当成当前应用一定能达到的数字。

本轮据此确定第一轮实现候选，而不是提前宣布最终最优值：

```text
网络/内存/背压基础 Block：继续使用 64 KiB。
磁盘第一候选 batch：       4 × 64 KiB = 256 KiB。
磁盘第二候选 batch：      16 × 64 KiB = 1 MiB。
最大等待：                 有界微秒级 timer 或 Chunk 结束立即 flush。
持久化：                   完整 Chunk/明确 group 后 fdatasync，不按 64 KiB fsync。
```

选择 256 KiB 作为第一候选，是因为它在 QD8 已进入设备平台，同时只聚合 4 个现有 Block，内存、首块等待和多连接公平性
风险低于直接等待 1/4 MiB。实现时应保留固定 64 KiB BlockPool，通过 `pwritev` 或 grouped write 动态聚合 1/4/16 个
相邻 Block；不要让每个连接动态申请不同大小的底层 slab。最终默认值仍需由 MiniDriver RF=1/RF=2、c1/c4/c8、
64 KiB/16 MiB/256 MiB 对象和三机网络矩阵决定。

#### 6.8.10 单 DataNode 持续上传分段定位（2026-08-26）

为了把“约 200 MiB/s”继续拆到具体代码阶段，本轮在新 SSD 上运行单 Gateway、单 DataNode 的隔离测试，并使用当前
已有的结构化日志，而不是仅凭总吞吐猜测瓶颈：

```text
数据目录：/data-ssd 上的全新隔离目录
拓扑：1 Gateway + 1 DataNode；loopback；无 Replica Body
对象：16 MiB = 4 × 4 MiB Chunk
负载：8 个上传文件、chunk-window=2、global-chunk-budget=8、15 轮
服务端：2 个 I/O thread、2 个 Disk Worker、128 × 64 KiB Block
总量：120 个对象，1.875 GiB 逻辑数据
```

120/120 个对象全部上传成功。文件级上传延迟为：

```text
median / P95 / P99 = 346.9 / 646.6 / 678.8 ms
```

逐轮聚合吞吐前 10 轮平均约 `221.40 MiB/s`，后 5 轮平均约 `176.21 MiB/s`，下降约 20.4%。它再次证明短轮次会先
利用 guest/host 页缓存，而持续写入跨过 dirty/writeback 阈值后才暴露稳定写路径。当前仍没有 `fdatasync`，因此这里是
非 durable 的持续写基线，不是掉电持久化成绩。

DataNode 异步日志在进程停止前记录了 475 个 `chunk_complete`；Gateway 则记录了全部 480 个 Chunk Commit。少数 DataNode
事件未进入汇总是停止时异步日志尚未全部 flush，不代表对象失败。可观测 Chunk 的分段如下：

| 阶段 | P50 | P95 | P99 | 最大值 | 说明 |
|---|---:|---:|---:|---:|---|
| Chunk 总时间 | 67 ms | 139 ms | 355 ms | 372 ms | 几乎全部落在 Body 接收/背压阶段 |
| Body receive | 66 ms | 138 ms | 354 ms | 371 ms | 包含等待磁盘任务完成后恢复 socket 的时间 |
| SHA-256 update 累计 | 17.345 ms | 24.827 ms | 31.136 ms | 37.291 ms | 每个 4 MiB Chunk 的累计 CPU hash 时间 |
| `pwrite` syscall 累计 | 3.872 ms | 6.337 ms | 61.706 ms | 228.844 ms | P99/最大值出现明显 writeback 长尾 |
| Physical index | 13 us | 31 us | 39 us | 141 us | 不是当前主瓶颈 |
| Gateway Commit RPC | 0 ms | 1 ms | 5 ms | 10 ms | 不是当前主瓶颈 |

Gateway 自身的控制面时间更小：

| 操作 | P50 | P95 | P99 |
|---|---:|---:|---:|
| Route plan | 20 us | 50 us | 183 us |
| Chunk Commit | 69 us | 176 us | 288 us |
| File Commit | 138 us | 311 us | 481 us |
| Session Create | 40 us | 93 us | 1.620 ms |

因此，对 16 MiB 大对象而言，“两次 LevelDB Write、逐 Chunk Route、HTTP 控制请求”不是本轮约 200 MiB/s 的第一限制。
它们仍会显著影响 64 KiB 小对象 object/s，但不能解释 4 MiB Chunk 的 67–355 ms 长尾。

最后一个资源快照记录：

```text
Disk tasks completed                 41,256
queue wait total / max               22.132 s / 225.891 ms
worker work total / max              11.603 s / 223.925 ms
平均 queue wait / worker work        536 us / 281 us per task
queue peak                           8 tasks
BlockPool 最低可用                   3 / 128 blocks
BlockPool peak leased                8 MiB / 8 MiB
I/O EventLoop timer lag 最大         8 ms
```

累计排队时间约为累计 worker 工作时间的 1.91 倍。按 475 个已记录 Chunk 粗略折算，当前每个 4 MiB Chunk 产生约 87 个
DiskExecutor task，平均有效片段约 47 KiB，而不是固定 64 KiB。原因在代码中很明确：`HttpContext` 每交付一个不超过
64 KiB 的可消费片段，`ChunkDiskWritePipeline::push()` 就复制到一个 Block；随后该 Pipeline 通过
`appendInFlight_` 串行提交一个 task，task 内依次执行一次 SHA update 和同步 `pwrite`。64 KiB 是上限，不保证 TCP/HTTP
每次都交付完整 64 KiB。

定位结论按证据强弱排列：

```text
已排除为当前第一瓶颈：loopback 网络带宽、Gateway Route/Commit、Physical index、EventLoop timer。
已确认的热点：大量约 47 KiB 的串行 SHA+pwrite task、两 Worker 的队列等待、脏页回写时 pwrite 百毫秒长尾。
次级成本：SHA-256 每 4 MiB 平均约 17.8 ms；它很重要，但单独仍不能解释全部 Body 时间。
仍需单变量验证：256 KiB grouped write、Disk Worker 2/4/8、fdatasync/group commit，以及客户端 Keep-Alive。
```

`perf stat` 本轮因虚拟机 `kernel.perf_event_paranoid=4` 且进程没有 `CAP_PERFMON` 被拒绝。没有为了压测修改系统安全参数；
上述结论全部来自应用结构化指标、客户端时间和已完成的 iperf3/fio 对照。若后续需要函数级 CPU 火焰图，应单独获得
主机权限后再采集，不能把缺失的 perf 数据伪装成已经证明的 CPU 百分比分布。

#### 6.8.11 DataNode On-CPU 火焰图（2026-08-26）

在主机侧将 `kernel.perf_event_paranoid` 临时从 4 调整为 2 后，`perf` 已能采样当前用户进程。不过 Hyper-V 没有向 Guest
暴露硬件 PMU，`cycles:u` 和 `instructions:u` 显示 `<not supported>`；因此本轮改用软件事件 `cpu-clock:u`：

```text
perf record -F 99 -e cpu-clock:u -g --call-graph fp -p <datanode-pid> -- sleep 30
```

构建为 `RelWithDebInfo`，包含 `-O2 -g -fno-omit-frame-pointer`。采样窗口为 28.921 秒，得到 2,683 个 sample、约
27.101 CPU 秒，lost sample 为 0。同期负载为 400 个 16 MiB 对象、8 文件并发、8 Chunk budget；400/400 成功：

```text
文件 median / P95 / P99       330.4 / 570.3 / 694.5 ms
50 轮聚合吞吐平均              211.68 MiB/s
前 10 轮 / 后 10 轮            229.88 / 207.48 MiB/s
```

这些吞吐数据包含采样开销，只用于确认 profile 期间负载正常，不能替换无 profiler 的正式性能基线。生成的 SVG 位于：

[MiniDriver DataNode On-CPU Flame Graph](profiling/MINIDRIVER_DATANODE_ONCPU_FLAMEGRAPH_2026-08-26.svg)

按 leaf instruction pointer 所属 DSO 汇总 CPU 时间：

| DSO / 热点 | On-CPU 占比 | 解释 |
|---|---:|---|
| `libcrypto.so.3` | **91.09%** | DataNode 在 Disk Worker 内执行 `EVP_DigestUpdate(SHA-256)` |
| `libc.so.6` | 5.11% | 主要包含内存复制、分配、锁等 |
| `__memmove_evex_unaligned_erms` | 3.02% | Body 复制进固定 Block 等路径 |
| `minikv_v2_datanode` | 2.87% | EventLoop、Pipeline、Executor 调度等项目自身代码 |
| `libstdc++` + `[vdso]` | 0.86% | 标准库与用户态系统辅助页 |

按线程拆分也吻合代码结构：两个 Disk Worker 在 `libcrypto` 中分别占总样本的 46.07% 与 45.02%；两个 I/O thread
主要落在 `libc` 与 MiniDriver 网络/EventLoop 代码。系统 `libcrypto` 缺少完整内部符号/帧指针，因此图中 SHA 内部叶子显示
为地址，但 DSO 归属和调用入口仍可由 `WriteSession::append → EVP_DigestUpdate` 与已有 `sha_update_us` 指标交叉确认。

本轮同步结构化日志记录 1,595 个可观测 Chunk：SHA update 平均约 17.506 ms/4 MiB，`pwrite` P99/最大值约
92.015/137.021 ms；最后快照中 135,338 个 Disk task 的平均 queue wait/work 约为 529/279 us。两组证据回答的是
不同问题：

```text
On-CPU：进程真正占用 CPU 时，91% 花在 SHA-256，3% 左右花在 memmove。
Off-CPU/墙钟：线程还会因小任务排队、pwrite 与脏页回写而等待；这些等待不会出现在 On-CPU 火焰宽度中。
```

采样期间 DataNode 只消耗约 `27.101 / 28.921 = 0.94` 个 CPU core，并没有用满虚拟机的 8 个 vCPU。因此当前不能下结论
“总吞吐完全被 CPU 算力打满”；准确结论是：**SHA 是已确认的第一 CPU 热点，磁盘队列/writeback 是已确认的墙钟长尾**。
优化 grouped write 以后，SHA 很可能成为更明显的吞吐上限，两者需要分别处理和复测。

Guest 的 `/proc/cpuinfo` 未暴露 `sha_ni`。独立 `openssl speed -bytes 4194304 -evp sha256` 测得约
`314,921 kB/s`（约 300 MiB/s）单流 SHA-256；应用内每 Chunk 约 17.5 ms 对应约 228 MiB/s。后续 CPU 方向应优先验证
虚拟机 CPU feature、OpenSSL/provider 与批量 hash，而不是手写 SHA；I/O 方向仍优先验证 256 KiB grouped write 与
Disk Worker 2/4/8。

该图仅为 On-CPU 用户态图。若要生成严格的 off-CPU 火焰图，需要 root 权限采集 `sched:sched_switch`/block tracepoint 或
eBPF 调度事件；不能用本图中 `pwrite` 框的宽度代替 I/O 等待时间。测试完成后应恢复：

```bash
sudo sysctl -w kernel.perf_event_paranoid=4
```

#### 6.8.12 `buffered` 与 per-Chunk durable 基线（2026-08-27）

MiniDriver 3.0 已实现可选择的持久化策略，并在同一块新 SSD 上用隔离目录完成首轮严格对照：

```text
buffered：   pwrite 成功 + 非同步 LevelDB index 后返回；不承诺断电持久化
chunk_sync：pwrite → SHA-256/长度验证 → fdatasync(dataFd)
            → LevelDB physical index sync=true → 返回成功

拓扑：1 Gateway + 1 DataNode，loopback，无 Replica Body
数据：/data-ssd/minidriver-v3-durability-20260827/{buffered,chunk_sync}
配置：4 MiB Chunk，2 I/O thread，2 Disk Worker，128 × 64 KiB Block
负载：16 MiB c1/c4/c8/c16 各 5 轮；64 MiB c4 持续 10 轮
结果：两种模式共 370/370 个对象成功，约 9.53 GiB 逻辑写入
```

单 DataNode 仍处于 Gateway 目标 RF=2 的测试配置，所以对象可能保持 `PROTECTING`；这里测的是 RF=1 物理数据路径和
durability 开销，不是正式高可靠拓扑的 File Commit 语义。

##### 文件级吞吐与延迟

| 负载 | buffered 吞吐 | chunk_sync 吞吐 | buffered median / P95 / P99 | chunk_sync median / P95 / P99 |
|---|---:|---:|---:|---:|
| 16 MiB c1 | 72.448 MiB/s | 61.168 MiB/s | 118.377 / 153.852 / 153.852 ms | 157.456 / 179.051 / 179.051 ms |
| 16 MiB c4 | 206.422 MiB/s | 147.926 MiB/s | 175.668 / 199.395 / 208.984 ms | 301.863 / 342.152 / 346.938 ms |
| 16 MiB c8 | 243.068 MiB/s | 156.382 MiB/s | 335.465 / 406.413 / 409.093 ms | 632.439 / 673.204 / 689.358 ms |
| 16 MiB c16 | 256.172 MiB/s | 158.396 MiB/s | 607.607 / 740.519 / 784.224 ms | 1141.418 / 1347.689 / 1381.455 ms |
| 64 MiB c4，10 轮 | **221.324 MiB/s** | **143.576 MiB/s** | 714.467 / 781.679 / 787.379 ms | 1281.111 / 1921.542 / 1937.452 ms |

表中吞吐是各轮 `round aggregate` 的均值，不是用文件延迟反推。持续 64 MiB/c4 的 `chunk_sync` 比 `buffered` 低
约 35.1%，P95 高约 145.8%。16 MiB 从 c4 增到 c16 时，durable 吞吐只从 147.926 增到 158.396 MiB/s，文件 P95
却从 342.152 增到 1347.689 ms；因此增加客户端并发无法消除同步屏障，只会扩大等待队列。

##### DataNode 分段指标

DataNode 异步日志分别落下 1219/1217 条 `chunk_complete`，比理论 1220 条略少，是服务停止时最后几条异步日志尚未
flush；客户端对象全部成功。带完整新字段的记录统计如下：

| 指标（每 4 MiB Chunk） | buffered | chunk_sync |
|---|---:|---:|
| data sync operations | 0 | 1217/1217，每 Chunk 1 次 |
| index sync operations | 0 | 1217/1217，每 Chunk 1 次 |
| `fdatasync` P50 / P95 / P99 / max | 0 | 6.119 / 28.374 / 45.872 / **615.538 ms** |
| Physical index P50 / P95 / P99 | 0.011 / 0.026 / 0.043 ms | 1.596 / 3.807 / 6.924 ms |
| Chunk total P50 / P95 / P99 | 73 / 303 / 391 ms | 160 / 630 / 788 ms |
| disk pause P50 / P95 / P99 | 57 / 272 / 347 ms | 112 / 531 / 730 ms |

`chunk_sync` 同时增加 data barrier 和同步 WAL/index 发布；它每 GiB 大约执行 256 次 data sync 和 256 次 index sync。
最大 615 ms 的 `fdatasync` 说明 guest page cache、Hyper-V 虚拟盘和宿主机写回仍会形成长尾。Linux 层面以
`fdatasync` 成功作为明确 ACK 边界，但这不是物理断电/宿主崩溃注入测试，后者仍需单独验收。

##### 当时结论与后续进展

```text
已经完成：明确 buffered 与 chunk_sync 的 ACK 语义、顺序、指标、单测和真实性能下界。
已经证明：非 durable 的约 220 MiB/s 不能当成 durable 成绩；per-Chunk durable 约 144 MiB/s。
不能采用：通过把并发从 c4 加到 c16 来“优化”持久化，吞吐收益很小而 P95/P99 大幅恶化。
当时待办 P3：同一 Chunk 内做 256 KiB PendingWriteBatch + pwritev，降低小 task/syscall/队列开销；现已完成，见 6.8.13。
P4 已完成：按 volume 实现 8 MiB / 8 items / 2 ms Group Commit，合并 data/index sync 并保持 ACK fencing，
结果见 6.8.14。
```

可复跑脚本为 `tools/run_v3_durability_benchmark.sh`；它要求 `/data-ssd/minidriver-v3-durability-*` 专用根目录，保存每档
`runs.csv/summary.csv/summary.json/console.log` 与 DataNode 结构化日志，不读取或删除仓库 `data/`。

#### 6.8.13 P3 同 Chunk 聚合写与 `pwritev` 验收（2026-08-27）

P3 保留网络与副本协议不变，只改变 Primary/Replica 各自的本地磁盘调度：

```text
HttpContext 任意 1～64 KiB Body fragment
  → SharedBlock（ReplicaTransport 可立即共享）
  → 同一 Chunk 的 PendingWriteBatch
  → bytes >= target / Body end / deadline / high watermark
  → 一个 DiskExecutor task
  → SHA 按 slice 顺序 update + pwritev 连续 extent
```

默认与回退配置：

```text
MINIKV_V3_WRITE_BATCH_MODE=pwritev       # single 可回退旧路径
MINIKV_V3_WRITE_BATCH_BYTES=262144       # 256 KiB
MINIKV_V3_WRITE_BATCH_DELAY_US=1000      # 1 ms
```

`FastDataStore::appendBatch()` 会处理 `EINTR`、短写、iovec 内部偏移以及超过 `_SC_IOV_MAX` 后的多次调用。一次请求仍只
对应一个逻辑 Chunk；不同 Chunk/extent 不会进入同一个 `pwritev`。`finishInput` 强制提交不足阈值的尾批，慢客户端由
deadline 提交；1 MiB 高水位和全局 BlockPool 继续限制 SharedBlock 生命周期。

##### 16 MiB/c8 参数矩阵

每档均为 1 Gateway + 1 DataNode、buffered、2 Disk Worker、5 轮 × 8 对象，全部 `40/40` 成功：

| 模式 | round aggregate 均值 | 文件 median / P95 / P99 |
|---|---:|---:|
| single，旧逐片段 pwrite | 227.528 MiB/s | 378.194 / 409.995 / 418.354 ms |
| pwritev 64 KiB / 1 ms | 233.024 MiB/s | 354.098 / 375.212 / 384.251 ms |
| pwritev 128 KiB / 1 ms | 239.262 MiB/s | 362.827 / 390.119 / 405.378 ms |
| **pwritev 256 KiB / 1 ms** | **246.998 MiB/s** | **319.094 / 371.031 / 382.624 ms** |
| pwritev 512 KiB / 1 ms | 246.486 MiB/s | 338.717 / 389.781 / 396.262 ms |
| pwritev 256 KiB / 0 ms | 241.102 MiB/s | 346.508 / 368.006 / 381.321 ms |
| pwritev 256 KiB / 2 ms | 239.828 MiB/s | 346.924 / 384.268 / 389.674 ms |

256 KiB/1 ms 相对 single：吞吐 `+8.6%`，median/P95/P99 分别 `-15.6%/-9.5%/-8.5%`。512 KiB 没有继续提高
吞吐，延迟也弱于 256 KiB；0 ms 更容易形成不足目标大小的小批，2 ms 则增加等待，因此当前选择 256 KiB/1 ms。
EventLoop timer 当前只有毫秒粒度，500 us 会向上取整为 1 ms，所以本报告没有制造一个虚假的 0.5 ms 独立结果。

##### Disk task 是否真的下降

结构化 Chunk 日志显示：

| 模式 | Disk batches/Chunk P50 / P95 | batch peak bytes P50 / P95 |
|---|---:|---:|
| single | 78 / 121 | 64 / 64 KiB |
| pwritev 128 KiB | 32 / 34 | 130 / 192 KiB（约） |
| **pwritev 256 KiB** | **16 / 18** | **260 / 320 KiB（约）** |
| pwritev 512 KiB | 10 / 12 | 515 / 576 KiB（约） |

256 KiB 已把 batch P50/P95 降低约 79.5%/85.1%，达到“每 4 MiB Chunk 不高于约 20 个 Disk task”的 P3 目标。
峰值可能比配置多一个网络 Block，是因为 flush 条件是累计字节达到阈值，而实际 fragment 并不固定为 64 KiB。

##### 持续写和 durable 边界

64 MiB/c4、10 轮的同次 A/B：

| durability | 写路径 | 吞吐 | median / P95 / P99 |
|---|---|---:|---:|
| buffered | single | 221.321 MiB/s | 712.618 / 769.985 / 793.083 ms |
| buffered | pwritev 256 KiB | 225.120 MiB/s | 687.810 / 820.422 / 883.542 ms |
| chunk_sync | single | 150.349 MiB/s | 1271.415 / 1313.093 / 1357.087 ms |
| chunk_sync | pwritev 256 KiB | 153.390 MiB/s | 1249.552 / 1310.852 / 1322.625 ms |

buffered 持续吞吐只提高 1.7%，且本轮 P95/P99 反而波动上升；chunk_sync 吞吐提高 2.0%，P99 下降约 2.5%。因此
P3 的确定收益是减少 task/syscall、改善 c8 短中负载，而不是宣称已经解决持久化长尾。per-Chunk
`fdatasync + LevelDB sync` 仍是 P4 必须处理的主瓶颈。

##### RF=2 + chunk_sync 端到端正确性

额外启动 1 Gateway + 2 DataNode，两节点都使用 `pwritev 256 KiB/1 ms + chunk_sync`：

| 对象大小 | 样本 | 成功 | 上传 median / P95 | 下载 median / P95 |
|---:|---:|---:|---:|---:|
| 64 KiB | 8 | 8/8 | 20.096 / 28.319 ms | 3.339 / 6.833 ms |
| 4 MiB | 8 | 8/8 | 131.287 / 155.849 ms | 56.155 / 83.418 ms |
| 16 MiB | 8 | 8/8 | 444.721 / 454.497 ms | 200.251 / 263.341 ms |
| 64 MiB | 8 | 8/8 | 1706.098 / 1727.859 ms | 688.185 / 748.331 ms |
| 256 MiB | 4 | 4/4 | 6976.633 / 6998.670 ms | 2619.901 / 2677.677 ms |

`36/36` 个对象全部完成 Session、Route、Primary→Replica、两端 data/index sync、File Commit、manifest、下载和最终
SHA-256 对比。该测试使用同一虚拟 SSD 上的两个 DataNode，只证明协议/数据正确性，不代表跨机 RF=2 性能。

P3 参数矩阵原始结果位于 `/data-ssd/minidriver-v3-p3-20260827`，RF=2 smoke 位于
`/data-ssd/minidriver-v3-p3-rf2-smoke-20260827`。可复跑脚本为 `tools/run_v3_write_batch_benchmark.sh`；它拒绝复用已存在
的结果根目录，避免把旧数据、页缓存状态和新一轮结果静默混合。

#### 6.8.14 P4 Group Commit 与 durable ACK 验收（2026-08-28）

P4 在 P3 的 `pwritev 256 KiB/1 ms` 之上增加每个 `FastDataStore` 独立的 `DurabilityCoordinator`：

```text
Chunk pwritev + SHA/length 完成
  → 分配 writeSequence，进入有界 pending
  → bytes/items/delay 任一阈值形成不可变 batch
  → 独立 sync worker 执行一次 fdatasync(dataFd)
  → 一个 LevelDB WriteBatch(sync=true) 发布本批全部 extent
  → 成功后以同一个 durableSequence 唤醒本批 waiter
  → DataNode 才向副本上游/Gateway 返回成功
```

sync worker 与两个 `DiskWriteExecutor` worker 分离，避免所有 Disk Worker 同时阻塞在 barrier 上而无法完成后续 pwrite。
pending 默认上限为 64 MiB/64 items：请求读取 Body 前先做 admission，enqueue 时再次校验硬边界。启动时根据已发布的
`e:` extent 重建 free extent 集，因此 data sync 成功但 index sync 失败留下的 orphan 不会永久泄漏空间。

##### RF=1 持久化与参数矩阵

共同条件为 1 Gateway + 1 DataNode、新 SSD、loopback、4 MiB Chunk、2 Disk Worker、16 MiB/c8 各 5 轮；三种主要模式
另运行 64 MiB/c4 10 轮持续写。所有正式样本均成功：

| 模式 | 16 MiB/c8 吞吐 | median / P95 / P99 | 64 MiB/c4 持续吞吐 | 持续 median / P95 / P99 |
|---|---:|---:|---:|---:|
| buffered | 247.956 MiB/s | 324.078 / 364.105 / 389.646 ms | 227.130 MiB/s | 673.909 / 950.821 / 1026.348 ms |
| per-Chunk sync | 149.802 MiB/s | 649.234 / 712.436 / 740.808 ms | 153.435 MiB/s | 1218.066 / 1331.507 / 1370.294 ms |
| **group 8 MiB/8/2 ms** | **212.604 MiB/s** | **386.096 / 591.904 / 597.028 ms** | **181.138 MiB/s** | **969.020 / 1048.976 / 1127.199 ms** |

相对 per-Chunk sync，8 MiB 候选在 c8 的吞吐提高 41.9%，median/P95/P99 分别下降约
40.5%/16.9%/19.4%；持续写吞吐提高 18.1%，median/P95/P99 分别下降约 20.4%/21.2%/17.7%。它仍比 buffered
持续吞吐低约 20.3%，这是明确 durability barrier 的成本，不能用 no-sync 数字掩盖。

16 MiB/c8 的单变量矩阵如下：

| Group Commit 阈值 | 吞吐 | median / P95 / P99 |
|---|---:|---:|
| 4 MiB / 4 items / 2 ms | 186.362 MiB/s | 515.880 / 597.039 / 600.294 ms |
| 8 MiB / 8 items / 1 ms | 192.870 MiB/s | 506.483 / 540.367 / 543.951 ms |
| **8 MiB / 8 items / 2 ms** | **212.604 MiB/s** | **386.096 / 591.904 / 597.028 ms** |
| 8 MiB / 8 items / 5 ms | 193.578 MiB/s | 483.888 / 575.308 / 581.023 ms |
| 16 MiB / 16 items / 2 ms | 210.462 MiB/s | 422.676 / 519.696 / 521.991 ms |

首轮 8 MiB/8/2 ms 的 798 条 Chunk 日志对应 553 次 data sync 和 553 次 index sync，即约 1.44 Chunk/sync，
相对 per-Chunk 减少约 30.7% sync。16 MiB 候选首轮可达约 2.74 Chunk/sync，但修复 pending 指标计数后使用新集群确认：
c8 为 206.704 MiB/s，64 MiB/c4 持续写只有 173.542 MiB/s，低于 8 MiB 候选的 181.138 MiB/s。因此当前保留
`8 MiB/8 items/2 ms`，不把“批次越大”误当成必然更快。

首轮矩阵暴露并修复了一个只影响指标/admission 计数的 unsigned underflow：batch 已取走后曾把 pending items 重置为
队列长度，完成时再次扣减。数据同步与 ACK 顺序未受影响；修复后用 16 MiB 候选重跑 797 条可观测 Chunk，pending peak
为 15、最终归零，不再出现下溢。相应 Store 单测也增加了完成后 pending bytes/items 必须为零的回归断言。

##### RF=2 端到端正确性

使用正式候选 `group_commit + 8 MiB/8/2 ms + pwritev 256 KiB/1 ms` 启动 1 Gateway + 2 DataNode：

| 对象大小 | 样本 | 上传 median / P95 | 下载 median / P95 | 端到端成功 |
|---:|---:|---:|---:|---:|
| 64 KiB | 8 | 20.234 / 29.462 ms | 2.742 / 3.796 ms | 8/8 |
| 4 MiB | 8 | 125.373 / 132.542 ms | 53.377 / 69.413 ms | 8/8 |
| 16 MiB | 8 | 392.150 / 433.454 ms | 200.553 / 328.736 ms | 8/8 |
| 64 MiB | 8 | 1557.076 / 1676.498 ms | 725.301 / 803.334 ms | 8/8 |

`32/32` 个对象、共 672.5 MiB 逻辑数据全部完成 Session、Route、Primary→Replica、两节点 durable ACK、File Commit、
manifest、下载和最终 SHA-256。两个节点各记录 176 个 Chunk，合计 352 个；Primary/Replica 角色均被覆盖，未发现
`durable=false`、错误 HTTP 状态或 checksum mismatch。该结果仍是同一虚拟 SSD 上的多进程正确性证明，不代表跨机
RF=2 吞吐。

##### 故障、恢复与边界测试

`test_fast_data_store_durability` 已覆盖：

```text
4 Chunk 合并为一次 data/index sync，并共享 durableSequence；
data fdatasync EIO：整批失败，physical index 不可见；
data sync 成功但 index sync 失败：整批失败，physical index 不可见；
重启扫描 orphan extent 并重建 free extent；
shutdown 停止 admission 后 drain pending batch；
sync 被阻塞时 pending 达上限，后续 admission 被拒绝；完成后 pending bytes/items 回到 0。
```

原始矩阵位于 `/data-ssd/minidriver-v3-p4-20260828`，修复后确认轮位于
`/data-ssd/minidriver-v3-p4-confirm-20260828`，RF=2 位于
`/data-ssd/minidriver-v3-p4-rf2-verified-20260828`。复跑脚本分别为
`tools/run_v3_group_commit_benchmark.sh` 与 `tools/run_v3_group_commit_rf2_smoke.sh`。当前默认 durability 仍为
`buffered` 以兼容旧行为；选择 `MINIKV_V3_DURABILITY_MODE=group_commit` 后，上述 8 MiB/8/2 ms 才作为当前候选生效。

#### 6.8.15 P5-A 协议契约与 HTTP Adapter 兼容验收（2026-08-28）

P5-A 只做行为等价的协议治理，不以本轮数据宣称吞吐提升。新增的 v2 Upload Capability 已把
`identityScheme/chunkId/objectId/objectVersion/generation/checksumType/checksumDigest` 纳入 HMAC 签名；DataNode 的
`HttpChunkUploadAdapter` 负责把 HTTP URL、Header、Capability 和 Replica Chain 解码为协议无关的
`ChunkWriteDescriptor`。现有物理路径仍显式使用 `cas-sha256 + sha256`，旧 v1 token 会被归一化成同一描述结构。
同时，磁盘和副本流水线已改为返回协议无关的 `StreamConsumeResult`，只有 DataNode HTTP 入口将其映射回
`HttpContext::BodyConsumeResult`，因此底层背压不再依赖 HTTP parser 类型。

协议单测覆盖 v1/v2 签发与验证、篡改拒绝、Route/本地节点不匹配，以及在 PhysicalStore 尚未支持双键前拒绝
`opaque-chunk-id`。随后使用 P4 的正式配置重新执行 RF=2 smoke：

```text
拓扑：1 Gateway + 2 DataNode，group commit 8 MiB/8 items/2 ms
对象：64 KiB / 4 MiB / 16 MiB / 64 MiB，各 8 个
结果：32/32 端到端成功，上传、双副本 durable ACK、Commit、下载和最终 SHA-256 全部通过
日志：两节点合计 352 个 Chunk；全部 identity_scheme=cas-sha256、checksum_type=sha256
      generation 均为非零，未发现 durable=false、chunk_failed 或协议字段不匹配
```

首次结果目录为 `/data-ssd/minidriver-v3-p4-rf2-p5-capability-20260828`。背压枚举解耦后又在
`/data-ssd/minidriver-v3-p4-rf2-p5-flow-20260828` 重跑相同矩阵，仍为 `32/32` 对象成功；两节点各 176 条、共 352 条
`chunk_complete`，全部带 `cas-sha256 + sha256` 和非零 generation，未发现失败或 `durable=false`。这证明升级 token、
解码边界和流控类型没有破坏现有 RF=2 数据路径；它尚未证明 opaque identity、CRC32C 或完全去 HTTP 的 Coordinator，
因为这些属于后续 P5-B/P5-C。

#### 6.8.16 P5-B/C：opaque identity 与 SHA-256/CRC32C durable A/B（2026-08-28）

P5-B/C 完成正确性闭环后，使用新 SSD 上的全新隔离目录执行了第一轮正式性能 A/B。测试固定
`opaque-chunk-id`，只切换 DataNode 热路径的 `checksumType`，避免把 CAS 路由差异误算成校验算法收益：

```text
拓扑：1 Gateway + 2 DataNode，两个 DataNode 共享同一块 /dev/sdc1
副本：RF=2，Primary → Replica；loopback，不代表跨机网络
持久化：group commit，data fdatasync → LevelDB sync=true → durable ACK
写入：pwritev，256 KiB + 1 ms；2 Disk Worker；8 MiB/8 items/2 ms group commit
准入：服务端 16 槽；客户端 chunk-window=2、global-chunk-budget=8
身份：opaque-chunk-id；客户端两组均计算相同 SHA-256/CRC32C 输入，最终下载仍以 SHA-256 验证
```

旧测试数据没有删除；本轮结果使用独立根目录
`/data-ssd/minidriver-v3-p5-ab-20260828`。五类负载在两种算法下共得到 `424/424` 条 benchmark 成功记录，未出现
503、校验失败或进程崩溃。

| 负载 | SHA-256 吞吐 | CRC32C 吞吐 | 吞吐变化 | SHA 上传 median/P95/P99 | CRC 上传 median/P95/P99 | P95 变化 |
|---|---:|---:|---:|---:|---:|---:|
| 16 MiB upload c4，5 轮 | 111.14 MiB/s | 116.25 MiB/s | +4.6% | 460.6/509.5/510.6 ms | 433.9/489.5/490.6 ms | -3.9% |
| 16 MiB upload c8，5 轮 | 113.97 MiB/s | 120.90 MiB/s | +6.1% | 754.6/981.4/1011.0 ms | 734.6/937.1/986.0 ms | -4.5% |
| 16 MiB upload c16，5 轮 | 119.01 MiB/s | 121.67 MiB/s | +2.2% | 1257.6/1852.8/1933.5 ms | 1408.4/1763.0/1851.6 ms | -4.8% |
| 64 MiB upload c4，8 轮 | 113.84 MiB/s | 122.10 MiB/s | +7.3% | 1821.6/1952.8/1956.3 ms | 1658.7/1817.6/1838.5 ms | -6.9% |
| 16 MiB mixed 4+4，5 轮 | 187.43 MiB/s | 206.29 MiB/s | +10.1% | 523.6/555.2/565.6 ms | 423.1/489.6/503.9 ms | -11.8% |

mixed 的下载 P95 为 SHA `346.5 ms`、CRC `341.9 ms`，基本不变；主要收益来自写入侧。设备计数也证明这是实际
durable 写入而非只停留在用户态：例如纯上传 c4 每组逻辑写入 320 MiB，`/dev/sdc1` 分别增加约 644.9/644.1 MiB，
与 RF=2 的约 640 MiB 物理数据量一致；64 MiB/c4 持续组每种算法均增加约 4127.7 MiB。

DataNode 结构化日志给出的分段证据为：

| 负载 | SHA checksum mean/P95 | CRC checksum mean/P95 | SHA/CRC Chunk P95 | SHA/CRC sync P95 | SHA/CRC 最大 disk pause |
|---|---:|---:|---:|---:|---:|
| upload c4 | 24.3/36.1 ms | 16.7/24.7 ms | 216/204 ms | 83.5/87.5 ms | 155/130 ms |
| upload c8 | 23.0/32.9 ms | 18.4/27.0 ms | 233/221 ms | 71.8/70.2 ms | 131/142 ms |
| upload c16 | 24.3/38.6 ms | 17.8/27.3 ms | 222/216 ms | 78.0/75.8 ms | 156/124 ms |
| sustained c4 | 23.8/34.9 ms | 17.9/26.3 ms | 222/204 ms | 74.4/66.9 ms | 178/131 ms |
| mixed 4+4 | 24.6/36.4 ms | 19.9/32.1 ms | 220/191 ms | 65.4/44.3 ms | 161/91 ms |

当前 CRC32C 是可移植的 byte-at-a-time table 实现，尚未使用 SSE4.2/ARM CRC 指令或成熟硬件加速库。因此它将每 Chunk
checksum CPU 平均降低约 19%～31%，但不是 CRC32C 的性能上限。即使如此，纯上传吞吐也只提高 2%～7%，说明当前
durable RF=2 的第一限制已经不是单独的 SHA-256；`fdatasync/index sync`、group wait、同盘双写和客户端预算共同决定
平台。CRC32C 可以作为 opaque identity 的传输/存储完整性候选，但不能靠它取代可选强内容指纹或端到端 SHA 验证。

并发曲线对动态调节更重要。在相同 `global-chunk-budget=8` 下，SHA c4→c8→c16 吞吐仅
`111.14→113.97→119.01 MiB/s`，CRC 仅 `116.25→120.90→121.67 MiB/s`；c16 的 P95 却比 c8 再增加约 89%。c8/c16
表示更多文件在客户端预算外排队，并不增加服务端即时 Chunk 工作集。因此当前静态安全默认仍是 4 个上传文件 × window 2，
动态控制器不应把“等待文件更多”误判成“磁盘并行度更高”。

本轮为后续自适应提供的是边界而不是现成算法：checksum 类型属于对象格式/策略，不能按瞬时负载动态切换；可动态调整的
应是 admission、group-commit batch/delay/items。控制器输入至少包括前台 P95/P99、group wait、sync EWMA、pending
bytes/items、disk pause/await 和吞吐斜率。当吞吐增幅低于约 5% 而 P95 增幅超过约 20% 时应停止升档或降低 admission，
不能继续堆积客户端请求。

复跑入口：

```bash
export MINIKV_V2_CLUSTER_SECRET='replace-with-a-temporary-test-secret'
export MINIKV_V3_P5_AB_ROOT=/data-ssd/minidriver-v3-p5-ab-rerun
tools/run_v3_p5_checksum_ab_benchmark.sh
```

#### 6.8.17 V3 前到当前 3.0 数据面的性能演进总表

这里必须把“数字变快/变慢”和“成功语义变强”放在同一张表中。早期 V3 前兼容数据面在 `pwrite` 和非同步 LevelDB
Write 返回后即可 Commit；当前 3.0 候选必须等 Primary、Replica 各自完成 data `fdatasync` 和同步 Physical Index
发布后才返回 durable ACK。两者都叫“上传成功”，但前者只表示进入 page cache/应用路径，后者才有明确的应用层
`fdatasync` 持久化边界；宿主断电/虚拟磁盘控制器语义仍需单独做故障验收。

##### 同一新 SSD、RF=2 的端到端产品演进

以下旧基线来自 6.8.6，当前基线来自 6.8.16。共同点是 1 Gateway + 2 DataNode、两个副本共享 `/dev/sdc1`、loopback、
4 MiB Chunk、客户端 `window=2/budget=8`；变化包含 durability、256 KiB `pwritev`、Group Commit、opaque identity 和
可替换 checksum。因此它回答“整个产品从以前到现在怎样”，不是只隔离某一行代码的微基准。

| 工作负载 | V3 前兼容路径：buffered + SHA | 当前 durable + SHA | 当前 durable + CRC32C | CRC 相对旧吞吐 | 旧 P95 → 当前 CRC P95 |
|---|---:|---:|---:|---:|---:|
| 16 MiB upload c4 | 175.6 MiB/s | 111.14 MiB/s | 116.25 MiB/s | -33.8% | 262.5 → 489.5 ms |
| 64 MiB upload c4 持续 | 177.54 MiB/s | 113.84 MiB/s | 122.10 MiB/s | -31.2% | 1115.6 → 1817.6 ms |
| 16 MiB mixed 4+4 | 279.4 MiB/s | 187.43 MiB/s | 206.29 MiB/s | -26.2% | upload 308.6 → 489.6 ms；download 338.9 → 341.9 ms |

因此，如果只问逻辑 MiB/s，当前 durable RF=2 确实比以前 no-sync 低约 26%～34%；这不是性能回归被隐藏，而是以前
把内核页缓存接纳速度当成了完成速度。当前每个逻辑字节仍写两份，并且每批必须跨越 data/index 两道 barrier。下载
P95 基本不变也说明下降集中在严格写入完成路径，而不是 HTTP、读取或 Gateway 全面变慢。

##### 同拓扑 RF=1 的单变量恢复过程

RF=2 历史矩阵没有逐阶段的完全同轮对照；RF=1 的 P2～P4 则清楚展示“持久化先付成本、再用工程优化拿回性能”：

| 阶段（64 MiB c4 持续） | 吞吐 | 相对说明 |
|---|---:|---|
| buffered，未强制落盘 | 227.130 MiB/s | 同轮性能上界，HTTP 200 不承诺 durable |
| per-Chunk data/index sync | 153.435 MiB/s | 每 4 MiB 两次同步，作为最严格下界 |
| 256 KiB pwritev + 8 MiB Group Commit | 181.138 MiB/s | 比 per-Chunk 提高 18.1%；达到 buffered 的 79.7% |

16 MiB/c8 同轮结果更明显：buffered `247.956 MiB/s`、per-Chunk `149.802 MiB/s`、Group Commit
`212.604 MiB/s`。Group Commit 比 per-Chunk 提高 41.9%，已经恢复到 buffered 的 85.7%；但它没有伪装成 100%，
剩余差距就是当前明确 durability contract 的实际成本。

##### 性能之外已经换来的能力

| V3 前兼容数据面 | 当前 3.0 数据面 |
|---|---|
| `pwrite`/page cache 后即可应用确认 | data sync → index sync → durable ACK |
| 4 MiB Chunk 约 80～90 个小 Disk task | 256 KiB `pwritev` 后通常约 16 个 batch |
| 没有显式 durable 合批与 ACK fencing | 8 MiB/8 items/2 ms Group Commit、pending 有硬上限 |
| `chunkHash` 同时是身份、路由和 SHA 校验 | opaque `chunkId`、objectVersion、generation、checksum 分层 |
| HTTP Handler 与写入协调生命周期耦合 | HTTP Adapter → `ChunkWriteCoordinator` → Store/Replica/Commit 接口 |
| buffered 成绩无法表达断电边界 | 日志可拆 checksum/pwrite/sync/group wait/durableSequence |

当前结论不是“3.0 比以前更快”，而是：3.0 在提供真实 durable RF=2 语义后，当前还能保持约
`114～122 MiB/s` 持续逻辑写和约 `187～206 MiB/s` 的 4+4 混合吞吐。下一阶段的目标是继续缩小这 31% 左右的
durability gap，而不是取消 sync 把数字恢复成虚假的 177 MiB/s。优先顺序应是硬件加速 CRC32C、跨两个本机进程的
同盘 sync 竞争定位、真实独立磁盘/三主机复测，以及 P8 有边界的 Group Commit/admission 自适应。

#### 6.8.18 RF=2 Group Commit 2～5 ms 固定窗口选择（2026-08-28）

为确认 2 ms 是否过于激进，本轮固定 P5-C 的其余配置，只扫描 `group_commit_delay_us=2000/3000/4000/5000`。测试顺序
采用 `2→5→3→4 ms`，每档使用全新集群目录，降低磁盘状态随时间单调变化造成的顺序误判：

```text
拓扑：1 Gateway + 2 DataNode，共享 /dev/sdc1，RF=2，loopback
协议：opaque-chunk-id + CRC32C
写入：256 KiB/1 ms pwritev；2 Disk Worker
持久化：8 MiB/8 items Group Commit，只改变 delay
准入：服务端 16 槽；客户端 chunk-window=2、global-chunk-budget=8
负载：64 MiB upload c4 × 6 轮；16 MiB mixed 4+4 × 6 轮
```

两类负载共 `288/288` 条 benchmark 记录成功。`queue P95` 为 Primary 上真正的
`group_wait - group_commit`；满批率按两个 DataNode 的 `durableSequence` 去重后统计 8 MiB/2-Chunk batch：

| Delay | 持续上传吞吐 | 上传 P95 | queue/commit/总 wait P95 | 持续写满批率 | mixed 吞吐 | mixed 上传/下载 P95 | mixed queue P95 | mixed 满批率 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 ms | 116.11 MiB/s | 1886.6 ms | 123.2/89.1/132.0 ms | 55.8% | **205.08 MiB/s** | 504.4/358.6 ms | **84.9 ms** | 24.2% |
| 3 ms | 119.64 MiB/s | **1810.2 ms** | **115.1/79.6/125.3 ms** | 52.2% | 201.72 MiB/s | **493.9/334.9 ms** | 110.0 ms | 27.2% |
| 4 ms | **121.59 MiB/s** | 1892.7 ms | 128.3/85.5/135.0 ms | 58.1% | 186.51 MiB/s | 600.8/350.5 ms | 154.5 ms | 37.7% |
| 5 ms | 116.45 MiB/s | 1875.1 ms | 121.9/88.2/128.8 ms | 58.0% | 197.47 MiB/s | 562.7/350.1 ms | 126.3 ms | 37.2% |

更长窗口确实让 mixed 满批率从 24.2% 提高到约 37%，但没有转化成更高的产品吞吐。4 ms 相对 2 ms 的持续上传均值
只提高 4.7%，mixed 吞吐却下降 9.1%，mixed 上传 P95 上升 19.1%；5 ms 同样没有取得稳定收益。3 ms 是最接近的折中，
但 mixed 吞吐仍下降 1.6%，Primary queue P95 上升 29.6%，其客户端 P95 小幅改善不足以证明可重复优势。

因此当前 RF=2 静态默认值**固定保留 2 ms**，不修改 `kDefaultGroupCommitDelayUs=2000`。这也回答了“2 ms 是否太严苛”：
它只是低负载下允许主动等待下一项的上限，不是 sync 完成 SLA；队列已有数据或达到 8 MiB 时会立即提交。当前百毫秒
长尾来自串行 batch 等待和共享虚拟 SSD 的 data/index sync，不是 2 ms timer 本身。

该结论只适用于当前虚拟 SSD、共享 volume 和 budget=8。未来在真实独立磁盘/三主机或不同 Chunk budget 上必须复测；
在此之前不启用动态 delay。复跑与汇总入口：

```bash
export MINIKV_V2_CLUSTER_SECRET='replace-with-a-temporary-test-secret'
export MINIKV_V3_P5_DELAY_ROOT=/data-ssd/minidriver-v3-p5-delay-rerun
tools/run_v3_p5_group_delay_benchmark.sh
tools/summarize_v3_p5_group_delay.py "$MINIKV_V3_P5_DELAY_ROOT"
```

### 6.9 当前推荐配置与容量含义

```bash
MINIKV_V4_MAX_ACTIVE_UPLOADS=8
MINIKV_V4_MAX_UPLOADS_PER_CLIENT=8
MINIKV_V4_MAX_ACTIVE_DOWNLOADS=8
MINIKV_V4_MAX_DOWNLOADS_PER_CLIENT=8
MINIKV_V4_DISK_WRITE_WORKERS=2
MINIKV_V4_DISK_WRITE_BLOCKS=128
MINIKV_V3_GROUP_COMMIT_DELAY_US=2000
```

```text
前端普通上传：4 个文件，每文件 window=2，global budget=8
前端普通下载：8 个文件
普通混合负载：4 上传 + 4 下载
批量模式：可以排队 6/8 个上传文件，但不增加服务端 Chunk budget
突发下载上限：16 已验证可完成，但默认仍为 8
```

“服务端 8 槽”与“8 个文件并发”不是同一个概念。RF=2 两节点下，一个复制 Chunk 同时占用两个节点各一个槽；
一个 `window=2` 文件占用两个复制 Chunk 槽，因此 8 槽的自然即时工作集是 4 个上传文件。

## 7. 新增观测的证据

本轮为 `DiskWriteExecutor` 新增了累计/最大排队等待与执行时间。在默认 `mixed c=8` 的 DataNode-A 快照中：

```text
completed tasks              = 14,830
queue-wait total / max       = 321,831 us / 3,991 us
work total / max             = 4,228,274 us / 4,902 us
queue peak                   = 2 tasks
block pool peak leased       = 4,521,984 B of 8,388,608 B
```

由此可得，这个短时 mixed 运行中 DiskWriteExecutor 的平均排队等待约 `21.7 us/task`，最大约 4 ms；平均工作时间约
`285 us/task`，最大约 4.9 ms。它不支持“短负载秒级上传 P95 主要由该 executor 队列造成”的假设。

但 6.8 的持续写入跨过脏页阈值后，平均 queue wait 已升至约 `1.17–1.22 ms/task`，最大约 `202–331 ms`，BlockPool
也实际耗尽。这两个结论不矛盾：短测测到页缓存吸收速度，持续测量才暴露同盘双副本的物理回写瓶颈。此时
DiskWriteExecutor 是背压传播位置，但根因首先是共享磁盘吞吐，不应仅靠增加 Worker 数掩盖。

同时，Gateway `route_plan` 内部日志一般为 `19–62 us`。它也不支持“Gateway HTTP 路由计算是当前主瓶颈”的
假设。它们不是端到端延迟，但足以从优先级上排除先重写 Gateway 或把 HTTP 改成 RPC 的必要性。

更合理的解释是：在 RF=2 的两节点测试中，默认前台上传 admission（以 Chunk 为单位）与客户端 chunk budget 使部分
上传文件等待前面的 Chunk 完成；文件级统计把这段等待纳入上传端到端时间。下载使用独立的下载配额与 sendfile 路径，
因而尾延迟较低。

## 8. 已确认问题与优先级

| 优先级 | 问题 | 证据 | 本轮结论 |
|---|---|---|---|
| P0 | 默认两个上传 Chunk 槽位在 RF=2 两节点上过保守 | U8-W2 仅即时接纳 1 文件；4/8/12/16 扫描在 8 附近出现吞吐拐点 | 下一轮以 8 为候选做三机标定，保留 4 作为回退值 |
| P0 | 前端文件并发必须与 Chunk 预算分开 | 8 槽时 c4 后上传吞吐平台化；c8 P95 接近翻倍 | 默认 4 上传文件 × window 2；批量任务在客户端排队 |
| P0 | 客户端未对 admission 503 退避重试 | 超额实验 18/20 显式失败 | 保留 503；增加 Retry-After/错误码与指数退避 |
| P0（已解决） | File Commit 原先没有数据/索引强制持久化边界 | P2/P4 已实现 `chunk_sync/group_commit`：data sync → index sync → durable ACK；RF=2 32/32 通过 | 默认仍保留 buffered 兼容；需要强持久化的部署显式选择 group_commit |
| P1 | 磁盘 worker 与 block pool 原来硬编码 | 仅能通过改 C++ 调参 | 已改为环境可配，下一轮单变量扫描 |
| P1 | 缺少任务级等待时间 | 原来只有瞬时队列/块池快照 | 已加入 queue/work total/max 指标 |
| P1 | 新 SSD 未提高短时上传平台 | 新旧盘 c4/c8 均约 180–200 MiB/s；新盘只消除了持续写回断崖 | 固定 SSD 与 8 槽，扫描 worker 与 Block 合并，同时记录 SHA/pwrite/复制阶段时间 |
| P1 | 单副本应用路径仍未吃满 SSD | MinIO c16 约 275 MiB/s，MiniDriver c16 约 235 MiB/s，fio QD8 约 488 MiB/s；小对象约 1212 对 621 object/s | 优先拆分 Session/Route/LevelDB、copy/SHA 与 pwrite 时间，不先更换协议 |
| P2 | c=8 结果波动很大 | 两轮 P95 明显不同 | 增加 warm/cold、轮数、持续时间与主机指标采样 |
| P2 | 大目录测试规模不足 | 当前控制面只有 200 个对象；catalog p16 约 742 QPS | 增加分页接口后测试 1 万/10 万目录，不外推到百万对象 |
| P2 | 尚无跨机网络/磁盘基线 | 本轮全为 loopback | 在三台 Tailscale 主机复测后再做网络结论 |

目前没有证据支持以下重工程作为第一步：替换 HTTP 为 RPC、引入 Kafka、引入 K8s、重写网络框架、把文件 Body 改由
Gateway 转发。它们不能解决已测出的 admission 排队，且会同时改变太多变量。

## 9. 本轮已实现的最小改动

### 9.1 DiskWriteExecutor 延迟观测

`DiskWriteExecutor::Metrics` 现在暴露：

```text
startedTasks
totalQueueWaitUs / maxQueueWaitUs
totalWorkUs / maxWorkUs
```

每秒 `event=resource_snapshot` 会输出这些字段。指标使用 `steady_clock`；只增加统计，不改变工作队列、写入顺序或
背压语义。

### 9.2 让磁盘执行器可按部署调参

DataNode 增加以下环境变量，默认行为保持不变：

| 环境变量 | 默认 | 有效范围 | 含义 |
|---|---:|---:|---|
| `MINIKV_V4_DISK_WRITE_WORKERS` | 2 | 1–32 | DiskWriteExecutor worker 数 |
| `MINIKV_V4_DISK_WRITE_BLOCKS` | 128 | 1–4096 | 64 KiB block 数；默认 block pool 为 8 MiB，最大为 256 MiB |

DataNode 启动时会记录 `event=datanode_runtime_config`，包含 I/O thread、上传/下载槽位、磁盘 worker/block 及
sendfile quantum。这样 Docker Compose 或三机配置不需要改代码即可记录和复现实验配置。

涉及代码：

```text
include/DataNode/DiskWriteExecutor.hpp
src/DataNode/DiskWriteExecutor.cpp
src/DataNode/datanode_main.cpp
```

验证：`cmake --build build -j8` 成功；`test_disk_write_executor`、`test_chunk_disk_write_pipeline`、
`test_write_admission` 与 `test_replica_upload_metrics` 通过。

### 9.3 Benchmark 小对象与完整矩阵支持

`parseSize()` 与 CLI usage 新增 `KiB`，使 benchmark 可以直接执行 `--sizes 64KiB`，不改变默认测试集合。
新增解析断言后重新构建，`test_benchmark_types`、`test_benchmark_cli` 通过。

本轮同时执行了 21 项与对象存储数据面直接相关的回归：FastDataStore read/region/delete、sendfile offset、Gateway
write lease/manifest/catalog/delete/cache/preflight、resource governor、write admission、DiskWriteExecutor、Chunk
pipeline、replica metrics 和 benchmark。`benchmark_http` 首次在文件系统沙箱中因不能绑定回环 socket 失败，使用允许
本地监听的环境单独重跑后通过；其余测试直接通过。

### 9.4 单 DataNode 对照基线支持

`tools/start_v2_local_benchmark_cluster.sh` 现在接受 `MINIKV_V2_BENCH_NODE_COUNT=1`，原有 2–4 节点行为和默认值不变。
脚本通过 `bash -n`，并实际启动单 Gateway/单 DataNode 完成上述 1064 条 benchmark 记录。这个选项只用于拆分单副本
数据路径成本，不改变 Gateway 的目标 RF，也不代表 V3-Lite 的正式部署范围缩成单节点。

## 10. 下一轮：先标定，再优化

本轮已经确定本机候选为 8 个复制 Chunk 槽位，完成 buffered/per-Chunk/group-commit、P3 聚合写及 RF=2 正确性对照。
P5-A 协议契约与 HTTP Adapter 已完成；下一轮进入 P5-B Coordinator/双身份存储，同时继续围绕 8 槽固定点做单变量
性能回归；每组至少 20 个有效样本，并保存 CSV、DataNode 资源快照、CPU/磁盘指标：

```text
A. 持久化：三模式与有界 group commit 已完成；P5 行为等价重构必须重复这组 durability 门禁
B. 测试隔离：benchmark 输入/下载输出与 DataNode 数据使用不同磁盘，避免客户端 I/O 污染服务端设备指标
C. disk executor：新 SSD 上固定 slots/budget=8，扫描 workers = 2 / 4 / 8、blocks = 128 / 256
D. 写入合并：P3 已固定 256 KiB/1 ms pwritev；后续各阶段保留 single 回退并持续做性能回归
E. 副本拓扑：两个 DataNode 分别使用旧盘和新盘，再进入三台机器的真正独立磁盘复测
F. I/O reactor：io threads = 2 / 4，其他固定为已选配置
G. 客户端可靠性：针对明确的 503/Retry-After 增加有限指数退避、抖动和相同业务请求重试
H. 长稳：4 上传 + 4 下载持续 30/60 分钟，记录 RSS、dirty/writeback、queue、pause、错误与吞吐漂移
I. 三机：真实 Tailscale/NIC/独立磁盘重复 upload/download/mixed 与 64/256 MiB 对象
J. Metadata：Catalog 分页后测试 1 万/10 万对象，分开冷读、热缓存与并发读
```

每个候选配置必须同时满足：

```text
100% 正确完成与 SHA-256 一致
无无界 block/queue/output-buffer 增长
mixed workload 的 upload/download P95 都不劣化超过预设阈值
超额请求被可识别、可退避的 admission 响应拒绝
```

只有找出“吞吐不再提高而 P95/错误开始恶化”的拐点，才能把对应配置写进 Docker Compose 的资源/环境配置。随后才做
客户端退避重试、可观测直方图、长稳压测、三机 Tailscale 基线和（必要时）线程/协议层优化。

## 11. 可复跑命令

以下命令使用新 SSD 上的独立目录。端口必须没有被已有服务占用；运行前确认路径属于测试目录，不要指向现有
`data/`、生产 DataNode volume 或其他用户数据。

```bash
cd ~/miniKV_v2/miniDriver
cmake --build build -j"$(nproc)"

export MINIKV_V2_CLUSTER_SECRET='replace-with-a-temporary-test-secret'
export MINIKV_V2_BENCH_CLUSTER_DIR=/data-ssd/minidriver-perf-rerun
export MINIKV_V2_BENCH_GATEWAY_PORT=28181
export MINIKV_V2_BENCH_NODE_A_PORT=29102
export MINIKV_V2_BENCH_NODE_C_PORT=29103
export MINIKV_V4_IO_THREADS=2
export MINIKV_V4_MAX_ACTIVE_UPLOADS=8
export MINIKV_V4_MAX_UPLOADS_PER_CLIENT=8
export MINIKV_V4_MAX_ACTIVE_DOWNLOADS=8
export MINIKV_V4_MAX_DOWNLOADS_PER_CLIENT=8
export MINIKV_V4_DISK_WRITE_WORKERS=2
export MINIKV_V4_DISK_WRITE_BLOCKS=128

tools/start_v2_local_benchmark_cluster.sh
build/bin/minikv_v2_bench local \
  --gateway 127.0.0.1:28181 \
  --work-dir "$MINIKV_V2_BENCH_CLUSTER_DIR/mixed-c8" \
  --sizes 16MiB --runs 20 --concurrency 8 \
  --chunk-window 2 --global-chunk-budget 8 --fixture-settle-ms 1000 --mode mixed \
  --remote-dir /perf/mixed-c8

rg 'event=resource_snapshot' "$MINIKV_V2_BENCH_CLUSTER_DIR"/node-*/logs
tools/stop_v2_local_benchmark_cluster.sh
```

复跑当前推荐工作点时，应同时设置服务端 slots 与客户端 budget 为 8；如果研究某个实现参数的贡献，则只替换
workers、blocks 或 I/O threads 中的一项，不要同时改变多个维度。

复跑 MiniDriver 3.0 当前两种持久化模式的完整 RF=1 对照矩阵：

```bash
export MINIKV_V2_CLUSTER_SECRET='replace-with-a-temporary-test-secret'
export MINIKV_V3_DURABILITY_BENCH_ROOT=/data-ssd/minidriver-v3-durability-rerun
tools/run_v3_durability_benchmark.sh
```

## 12. 与后续 V3 / Docker / K8s 的关系

性能标定与 V3 Raft 高可用是两条正交路线：

```text
本阶段：DataNode Body path 的并发、背压、磁盘、网络与客户端重试。
V3-Lite：小型 Metadata 的一致性、故障判定和副本修复。
Docker：固定二进制、环境变量、volume、端口与日志，保证本报告可复跑。
K8s/k3s：未来多服务部署与资源调度；不替代 admission/backpressure/性能测量。
```

因此本轮先不把 Raft、K8s、Kafka 或 RPC 改造混进压测；先把当前 V3 分支兼容数据面每个资源预算的拐点测清楚，才是之后做
高可用和容器化时不会失去性能基线的前提。
