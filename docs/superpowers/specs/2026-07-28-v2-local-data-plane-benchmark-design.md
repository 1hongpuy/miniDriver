# V2 本机数据面基准测试设计

## 目标

提供一个可重复运行的命令行基准工具，先在单台 Gateway 机器上测量完整 V2 上传和下载路径，排除 Tailscale、浏览器、代理和公网带宽的影响。

本轮覆盖的真实路径：

```text
Benchmark client
  -> Gateway 创建 Session / 获取路由
  -> Primary DataNode PUT Chunk
  -> Replica DataNode 链式复制
  -> Gateway Chunk Commit / File Commit
  -> Gateway Manifest
  -> DataNode GET Chunk
  -> Client 整文件 SHA-256 校验
```

## 非目标

- 不测试浏览器、Nginx、CORS 或 Blob 下载。
- 不测跨机器 Tailscale 带宽；该场景复用本工具在第二轮执行。
- 不引入多客户端并发、wrk、连接池、perf 火焰图或服务端资源采样。
- 不修改 Gateway/DataNode HTTP 协议和对象格式。
- 不把测试数据写入源码目录；输入与输出默认放入用户明确指定的临时目录。

## 本机拓扑

```text
minikv_v2_bench
  Gateway:      127.0.0.1:18081
  Primary:      127.0.0.1:9002      (node-a)
  Replica:      127.0.0.1:19003     (node-local-c)
```

Gateway 仍根据自身 NodeRegistry 做路由，Benchmark 不指定主副本或绕过 Token。

## 接口和命令

新增独立可执行文件：

```text
build/bin/minikv_v2_bench local \
  --gateway 127.0.0.1:18081 \
  --work-dir /tmp/minikv-v2-bench \
  --sizes 4MiB,32MiB,256MiB,1GiB \
  --runs 3 \
  --remote-dir /benchmark
```

参数含义：

- `--gateway`：Gateway HTTP 地址。
- `--work-dir`：生成输入文件、下载输出与结果文件的目录；不得使用 DataNode `dataDir`。
- `--sizes`：测试对象大小，默认 `4MiB,32MiB,256MiB,1GiB`。
- `--runs`：每个大小的独立重复次数，默认 `3`。
- `--remote-dir`：逻辑对象目录，默认 `/benchmark`。

第一版保留一个 `local` 子命令；以后再增加 `remote` 或 `upload`/`download` 子命令，不提前抽象。

## 客户端实现边界

工具复用 V2 控制面 HTTP 格式和 DataNode PUT/GET 格式，但不复用浏览器代码。它使用命令行进程内的顺序文件读取和固定 64 KiB 缓冲区：

```text
输入文件 chunk
  -> 增量 SHA-256
  -> POST routes
  -> PUT primary DataNode
  -> 等待 HTTP 响应
```

下载通过 Gateway manifest 获取副本列表。第一版按 manifest 中的副本顺序尝试 GET；读取完成后校验每个 Chunk Hash，并按索引写入下载目标文件。所有 Chunk 完成后计算整文件 SHA-256 与输入文件比对。

一次失败立即记录失败原因并结束该 run，不把失败吞掉或混入吞吐平均值。

## 采集指标

每次 run 写一行 `runs.csv`：

```text
run_id,size_bytes,upload_ms,upload_mib_per_s,download_ms,
download_mib_per_s,chunk_count,upload_ok,download_ok,error
```

每个大小写一行 `summary.csv`：

```text
size_bytes,runs,upload_min_ms,upload_median_ms,upload_mean_ms,
download_min_ms,download_median_ms,download_mean_ms,success_count
```

结果目录同时写入 `summary.json`，包含工具版本、启动时间、参数、Gateway 地址和各大小聚合指标，便于后续绘图与横向比较。

吞吐计算：

```text
MiB/s = object_size_bytes / 1024 / 1024 / elapsed_seconds
```

上传时间从创建 Session 前开始，到 File Commit 返回成功结束；下载时间从请求 manifest 前开始，到整文件 SHA-256 校验成功结束。

## 输入数据与正确性

工具生成可重复的二进制模式数据，不使用 `/dev/urandom` 作为基准输入，以便每次运行和失败重现成本低。每个 run 的输入文件名包含大小和 run 序号。

正确性条件：

1. 每个 PUT 返回 2xx。
2. File Commit 返回 2xx。
3. 每个下载 Chunk 的 SHA-256 等于 manifest 中的 hash。
4. 下载后整文件 SHA-256 等于对应输入文件。

任一条件失败则该 run 标记为失败，CSV 写出错误消息，进程最后以非零状态退出。

## 图表

第一版基准程序只生成标准 CSV/JSON，避免引入 Python、Matplotlib 或 Gnuplot 作为运行依赖。

第二步新增一个独立的离线绘图脚本，读取 `summary.csv` 生成：

```text
upload-throughput.svg
download-throughput.svg
latency-by-size.svg
```

图表脚本不参与上传下载，也不影响基准测试的可重复性。

## 验证

自动测试至少覆盖：

1. 大小参数解析：`4MiB`、`1GiB` 和非法输入。
2. 吞吐和中位数计算。
3. CSV 中失败 run 不计入成功统计。

手工验证：启动 Gateway、node-a、node-local-c 后，先运行单次 `4MiB`；确认上传、下载和整文件校验成功，再依次扩大到完整矩阵。

## 未来演进

同一 CSV 格式将作为后续对比基线：

```text
V2 本机单客户端
-> V2 Tailscale 外部客户端
-> V4 多 I/O Loop / 背压 / 磁盘线程池
-> V5 恢复和修复开销
```

这样性能结论有可复现实验，而不是只看一次网页上传速度。
