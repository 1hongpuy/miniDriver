# V2 私网性能基线执行单

> 前提：Gateway 和 DataNode 已使用本次源码重新构建；三台机器的 runtime dataDir、LevelDB
> 与 cluster secret 不共用。此文档记录命令和结果，不将运行数据、密钥或 build 目录提交 Git。

## 1. 构建与回归

在 Gateway 和每个 DataNode：

```bash
cd ~/miniKV_v2/miniDriver
cmake -S . -B build-perf -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTING=ON
cmake --build build-perf -j"$(nproc)"
ctest --test-dir build-perf --output-on-failure
git rev-parse --short HEAD
```

## 2. 私网拓扑记录

记录而不是猜测：

```text
Gateway private address: <GATEWAY_PRIVATE_IP>:18081
Node A private address:  <NODE_A_PRIVATE_IP>:9002
Node C private address:  <NODE_C_PRIVATE_IP>:9002
Benchmark host:           <BENCH_HOST>
```

DataNode YAML 的 `advertiseAddress` 必须填节点可被 benchmark 浏览器/客户端访问的地址。
只用于节点到节点复制的私网地址，不能替代浏览器直连所需的可达地址。

## 3. 执行矩阵

从一台有足够本地磁盘的 benchmark 主机执行。先用 4 MiB 验证链路，再使用 1 GiB 视频级输入。

```bash
BIN=~/miniKV_v2/miniDriver/build-perf/bin/minikv_v2_bench
GW=<GATEWAY_PRIVATE_IP>:18081
BASE=/tmp/minikv-v2-private-$(date +%Y%m%d-%H%M%S)

for concurrency in 1 2 4 8; do
  "$BIN" local --gateway "$GW" --work-dir "$BASE/c${concurrency}-4m" \
    --sizes 4MiB --runs 10 --concurrency "$concurrency" --remote-dir /benchmark
done

"$BIN" local --gateway "$GW" --work-dir "$BASE/c1-1g" \
  --sizes 1GiB --runs 3 --concurrency 1 --remote-dir /benchmark
```

V2 的固定双节点、每节点 2 写槽模型下，`concurrency=4/8` 是容量保护测试：出现 `503` 应计为
拒绝，不应与吞吐成功样本混合。稳定吞吐基线首先使用 `concurrency=1/2`。

## 4. 采集项

- `runs.csv`: 每个对象的 upload/download 时间、MiB/s、Chunk 数与错误。
- `summary.csv`: upload/download 的 min、median、P95、P99、mean。
- Gateway/DataNode 日志：`503`、DataNode SHA 校验失败、replica timeout、connection reset。
- 机器采样：`pidstat -dru -p <pid> 1`、`iostat -xz 1`、`ss -s`。

V2 仍没有生产级的 EventLoop lag、outputBuffer peak、replica pause duration 指标；这些需要
V4 的统一 metrics collector，不能用日志猜测为精确指标。

## 5. 通过标准

1. `ctest` 全绿。
2. `concurrency=1/2` 的每个成功对象，下载后的整文件 SHA-256 与输入一致。
3. 任何失败都在 `runs.csv` 和对应服务日志中有明确原因，不存在卡死或无响应。
4. 4/8 并发时，超出固定写槽的请求可预期返回 503，而非进程崩溃或元数据损坏。
