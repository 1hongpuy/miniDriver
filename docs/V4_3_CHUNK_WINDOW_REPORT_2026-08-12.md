# V4.3 有界 Chunk Window：实现与性能验证（2026-08-12）

## 结论

本轮为 C++ 压测客户端加入了同一文件的 `Chunk window=2`，并同时加入跨所有并发文件的
全局 Chunk 预算。默认 `window=1`，因此既有串行压测口径不变；启用 `--chunk-window 2`
后，一个文件最多两个 Chunk 同时执行 `routes + PUT`，但双节点、双副本、每节点两个上传槽的
当前集群仍将**总在飞 Chunk 限制为 2**。

这不是简单地把每个文件的并发翻倍。最初无全局限制的试跑中，两个文件各开 window=2 会同时发出
4 个 routes，请求超过双节点集群的有效写入容量并得到预期的 `503 + Retry-After`。加入预算后，
所有最终样本均成功，不再出现 503、异常 400 或 hash 错误。

在相同的真实 `/data`、双 DataNode、双副本、16 MiB、3 轮环境中，window=2 的文件上传
P50 改善明显：单文件从 **354.467 ms / 45.14 MiB/s** 降至 **127.236 ms / 125.75 MiB/s**；
两个文件并发从 **261.856 ms / 61.10 MiB/s** 降至 **156.499 ms / 102.24 MiB/s**。mixed 并发 4
也全部完成，上传和下载 P50 都降低。样本较小、测试为 loopback，结论是本机环境下存在强正向信号，
不是对跨机吞吐的承诺。

## 实现

### 客户端调度

新增 CLI：

```text
--chunk-window 1|2           # 默认 1，每文件未完成 Chunk 上限
--global-chunk-budget <N>    # 默认 2，整个 benchmark 轮次的总上限
```

每个待上传 Chunk 都在独立 worker 中完成下列完整操作：

```text
取得全局预算令牌
  → 重新打开输入文件并计算该 Chunk SHA-256
  → Gateway routes
  → PUT 到主 DataNode（后续仍走双副本）
  → 归还令牌
```

同一文件的 worker 数为 `min(chunkWindow, pendingChunks)`。Chunk 可以乱序完成；只有所有
Chunk 成功后才发送文件级 commit。任一 Chunk 失败后，该文件不再启动新的 Chunk，且不会 commit。

`ChunkBudget` 是一个条件变量保护的 RAII 信号量。它覆盖 routes 和上传的整个在飞区间，而不只是
PUT，因此突发的路由请求同样受到治理。预算是压测客户端的调度保护；Gateway/DataNode 原有的
WriteLease、上传槽和 `503 + Retry-After` 仍是服务端最终准入保护。

### 与网页客户端的关系

`www-v2/app.js` 先前已经有每文件 `MAX_INFLIGHT_CHUNKS_PER_FILE = 2` 和页面级
`MAX_ACTIVE_CHUNK_UPLOADS = 2`。本次主要补齐 C++ benchmark 的同等能力与可复现开关，
并使它的默认全局预算与当前网页客户端一致；没有改变网页协议或服务端 commit 语义。

## 最终性能矩阵

环境：8 vCPU、约 2.8 GiB RAM、`/data` ext4、同机 loopback；Gateway 1、DataNode 2、链式双副本；
每节点上传槽 2、下载槽 8、`MINIKV_V4_IO_THREADS=2`。对象为 16 MiB（4 × 4 MiB Chunk），
每档 3 轮，`--global-chunk-budget 2`。吞吐按 `16 MiB / 文件 P50` 换算。

| 模式 | 文件并发 | window | 成功/请求 | 上传 P50 / P95 | 下载 P50 / P95 |
|---|---:|---:|---:|---:|---:|
| upload-only | 1 | 1 | 3/3 | 354.467 / 365.072 ms; 45.14 MiB/s | — |
| upload-only | 1 | 2 | 3/3 | 127.236 / 143.772 ms; 125.75 MiB/s | — |
| upload-only | 2 | 1 | 6/6 | 261.856 / 331.428 ms; 61.10 MiB/s | — |
| upload-only | 2 | 2 | 6/6 | 156.499 / 258.053 ms; 102.24 MiB/s | — |
| mixed | 4 (2U + 2D) | 1 | 12/12 | 210.474 / 231.114 ms; 76.02 MiB/s | 210.879 / 259.600 ms; 75.87 MiB/s |
| mixed | 4 (2U + 2D) | 2 | 12/12 | 136.036 / 204.534 ms; 117.62 MiB/s | 181.999 / 222.403 ms; 87.91 MiB/s |

原始 CSV、日志和独立集群状态：

```text
/data/minikv-v2/v4-chunk-window-20260812/
  upload-c{1,2}-w{1,2}-governed/
  mixed-c4-w{1,2}-governed/
  cluster-governed/
```

实际命令形式：

```bash
MINIKV_V4_IO_THREADS=2 tools/start_v2_local_benchmark_cluster.sh
./build/bin/minikv_v2_bench local --gateway 127.0.0.1:<port> \
  --work-dir /data/minikv-v2/v4-chunk-window-20260812/<case> \
  --sizes 16MiB --runs 3 --concurrency <1|2|4> --mode <upload|mixed> \
  --chunk-window <1|2> --global-chunk-budget 2
```

## 正确性与回归

- 所有最终矩阵样本成功：upload c1 为 3/3、upload c2 为 6/6、mixed c4 为 12/12。
- window=2 的日志出现 Chunk 1/2、3/4 等乱序完成；文件均在全部 Chunk 成功后完成 commit，证明
  文件级顺序不依赖 Chunk 完成顺序。
- 完整 CTest：**47/48 通过**。唯一失败是 `v2_thumbnail_contact_sheet_ui` 缺少 Python
  `playwright` 模块；`benchmark_cli`、`benchmark_types`、`benchmark_http` 均通过。

## 受控过载的发现

在加入全局预算前，`concurrency=2, window=2` 会把最大在飞 Chunk 放大到 4。双副本文件的每个
Chunk 同时占用两个节点的写入资源，因而 Gateway 正确拒绝额外 routes。这个试跑不是性能结果，
但验证了“只设置每文件窗口”是不完整的：

```text
总在飞 Chunk = 文件并发 × 每文件窗口
```

因此当前推荐配置不是无条件 `window=2`，而是：

```text
每文件 window <= 2
全局预算 <= 已验证的集群数据面容量
```

在当前双节点、双副本、默认上传槽配置中，该预算为 2。扩充节点、提高经验证的上传槽，或改变
副本数后，应重新执行容量矩阵再调整该值。

## 后续优化

1. 将网页客户端的全局 Chunk 上限显式映射为服务端返回的容量/Retry-After 策略，而不是仅使用固定常量。
2. 为 benchmark 增加 Chunk P50/P95/P99 的按 window 分组输出，并在跨机网络下重复 window=1/2 矩阵。
3. 在保持 SharedBodyBlock、BlockPool、磁盘队列和副本连接池预算的前提下，才评估是否允许更高的
   全局预算；不能用取消背压换取并发。
4. 当 mixed 高比例下载或媒体后台任务加入后，再实施磁盘任务公平调度，检查 window=2 是否损害上传尾延迟。
