# V2 本机数据面基准测试

`minikv_v2_bench` 是 V2 的命令行端到端压测客户端。它真实调用 Gateway 会话、动态路由、DataNode 流式 Chunk 写入、链式副本、文件提交、manifest 和 Chunk 下载，不经过浏览器或 Nginx。

注意：只有 DataNode 向 Gateway 注册的 `advertiseAddress` 也是 `127.0.0.1` 时，测试才是严格的本机 loopback 基线。Gateway 在 `127.0.0.1`，但节点登记为 Tailscale IP 时，数据仍会经 Tailscale 地址回环，不能据此判断数据面本机性能。

## 启动前条件

推荐先启动隔离集群：

```bash
export MINIKV_V2_CLUSTER_SECRET="$(cat ~/minikv-v2-secrets/cluster.secret)"
./tools/start_v2_local_benchmark_cluster.sh
```

该脚本在 `/tmp/minikv-v2-bench-cluster` 创建独立的 LevelDB 和 DataNode 数据文件，并启动：

```text
Gateway:      127.0.0.1:18081
node-a:       127.0.0.1:9002
node-local-c: 127.0.0.1:19003
```

两个 DataNode 必须已经向 Gateway 注册为 `ONLINE`。工作目录必须与任意 DataNode 的 `dataDir` 分开。

结束隔离集群：

```bash
./tools/stop_v2_local_benchmark_cluster.sh
```

## 构建

```bash
cmake -S . -B build-perf -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTING=ON
cmake --build build-perf -j2
ctest --test-dir build-perf --output-on-failure
```

## 4 MiB 冒烟测试

```bash
./build-perf/bin/minikv_v2_bench local \
  --gateway 127.0.0.1:18181 \
  --work-dir /tmp/minikv-v2-bench-smoke \
  --sizes 4MiB \
  --runs 1 \
  --remote-dir /benchmark
```

成功时退出码为 `0`，并生成：

```text
/tmp/minikv-v2-bench-smoke/runs.csv
/tmp/minikv-v2-bench-smoke/summary.csv
/tmp/minikv-v2-bench-smoke/summary.json
```

## 完整本机基线

```bash
./build-perf/bin/minikv_v2_bench local \
  --gateway 127.0.0.1:18181 \
  --work-dir /tmp/minikv-v2-bench \
  --sizes 4MiB,32MiB,256MiB,1GiB \
  --runs 3 \
  --remote-dir /benchmark
```

这会执行 12 次独立上传和下载。每轮成功后会删除输入和下载临时文件，因此工作目录峰值约为最大测试对象的两倍（1 GiB 对象约 2 GiB）。DataNode 仍会保存对象；因此先确认两个 DataNode 各自有至少约 4 GiB 的可用空间。

## 结果字段

`runs.csv` 每行是一轮完整上传和下载：

```text
run_id,size_bytes,upload_ms,upload_mib_per_s,download_ms,
download_mib_per_s,chunk_count,upload_ok,download_ok,error
```

- `upload_ms`：从创建 Session 到 File Commit 成功的总时间。
- `download_ms`：从获取 manifest 到整文件 SHA-256 验证成功的总时间。
- `upload_mib_per_s`、`download_mib_per_s`：以完整对象大小计算。
- `error` 非空表示该行失败；失败行不进入 `summary.csv` 的均值和中位数。

`summary.csv` 按文件大小聚合成功运行的最小值、中位数、P95、P99 和均值。
分位数采用 nearest-rank：对于样本数很少的 smoke（例如 1 次），P95/P99 等于该唯一样本；
至少使用 10 次成功样本后再讨论尾延迟。`summary.json` 适合后续绘图脚本读取。

## 已验证的 loopback smoke

在 `RelWithDebInfo`、两 DataNode loopback 副本链下，2026-07-30 的单次 4 MiB smoke 为：

```text
upload:   13.977 ms, 286.190 MiB/s
download: 26.151 ms, 152.959 MiB/s
SHA-256:  input and downloaded object matched
```

这只证明 V2 端到端路径可工作，不能当作私网或公网吞吐结论。

## 并发解释

V2 当前每个 DataNode 的 `maxConcurrentWrites` 固定为 `2`。一个双副本 Chunk 会同时占用
主节点和副本节点各一个槽位；两个节点组成副本链时，理论上最多两个 Chunk 可以同时落盘。
因此 `--concurrency 4` 或 `8` 可能收到 Gateway `503 Retry-After: 1`。这表示容量保护生效，
不是随机上传错误。当前 benchmark 会记录为失败；它用于观察容量边界，不会把请求自动排队。

## 失败解释

```text
Gateway ... returned HTTP 400
  Gateway Session、路由或文件提交失败。检查 gateway 日志和节点 ONLINE 状态。

DataNode PUT returned HTTP ...
  上传 Token、链式副本或 DataNode 写入失败。检查 primary/replica DataNode 日志。

cannot download chunk N
  manifest 副本无法读取或 SHA-256 不一致。检查 DataNode physical_index 和 GET 路径。

HTTP socket timed out
  请求在设定时间内没有完成。先确认本机端口监听，再检查 DataNode 背压和副本链。
```

退出码 `1` 表示至少一轮失败，`2` 表示参数、工作目录或报告写入失败。

## 下一轮

本机基线稳定后，在 Windows/Tailscale 客户端使用相同的命令和大小矩阵，仅将 `--gateway` 改为 Gateway 的 Tailscale 地址。两次生成的 CSV 使用相同字段，可直接比较本机协议开销和真实网络带宽上限。
