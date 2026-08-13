# V2 本机数据面性能报告（2026-07-29）

## 范围与方法

本报告只测 V2 的数据面，不测 Tailscale、浏览器或 Nginx。测试时在同一台
Gateway 机器上启动一个隔离集群：

```text
Gateway   127.0.0.1:18181
DataNode A 127.0.0.1:19102
DataNode C 127.0.0.1:19103
```

构建使用 `build-perf` 的 `RelWithDebInfo`（`-O2 -g -fno-omit-frame-pointer`）。
`minikv_v2_bench` 对每个 worker 生成不同输入文件，完整执行：创建 Session、路由、
切片直传、链式副本、Gateway commit、manifest、Chunk 下载、Chunk SHA-256 与整文件
SHA-256 校验。临时数据仅写入 `/tmp`。

命令和原始 CSV/日志在：

```text
/tmp/minikv-v2-local-matrix-20260729-153322/
```

因此以下数据是“同机共享 CPU、页缓存与磁盘”下的 V2 基线，不能直接视为跨机器或
跨 Tailscale 的带宽上限。

## 吞吐结果

### 单文件延迟/速度

| 负载 | 成功 | 上传中位数 | 上传吞吐 | 下载中位数 | 下载吞吐 |
|---|---:|---:|---:|---:|---:|
| 64 MiB，1 worker，3 次 | 3/3 | 230.519 ms | 277.6 MiB/s | 360.898 ms | 177.3 MiB/s |
| 64 MiB，2 workers，6 次 | 6/6 | 316.397 ms | 202.3 MiB/s | 553.823 ms | 115.6 MiB/s |
| 64 MiB，4 workers，12 次 | 12/12 | 610.282 ms | 104.9 MiB/s | 987.919 ms | 64.8 MiB/s |
| 1 GiB，1 worker，1 次 | 1/1 | 6665.961 ms | 153.6 MiB/s | 9431.497 ms | 108.6 MiB/s |

“上传/下载吞吐”是单个文件阶段的吞吐。它不包含基准输入文件生成；上传仍包含 V2
控制面请求、逐 Chunk hash 与 commit，下载仍包含 manifest、逐 Chunk hash 与整文件
hash。

### 端到端聚合吞吐

端到端计时包含输入生成、上传及下载校验；它衡量一轮完整工作流的吞吐，不能与纯
网络带宽混为一谈。

| 负载 | 每轮聚合吞吐（MiB/s） | 中位数 |
|---|---|---:|
| 64 MiB，1 worker | 80.63 / 81.43 / 81.08 | 81.08 |
| 64 MiB，2 workers | 119.06 / 117.38 / 105.97 | 117.38 |
| 64 MiB，4 workers | 138.63 / 132.20 / 88.82 | 132.20 |
| 1 GiB，1 worker | 51.71 | 51.71 |

结论：2 worker 相对单 worker 将端到端吞吐提高约 45%，4 worker 最高轮次约提高
71%，但第三轮降至 88.82 MiB/s。并发能提高利用率，但不能线性扩展，而且尾部波动
已经明显。

`/usr/bin/time` 记录的 client 进程峰值 RSS：单 64 MiB 19.8 MiB、2 并发 31.6 MiB、
4 并发 56.0 MiB、单 1 GiB 15.8 MiB。这个数不包括 Gateway 和两个 DataNode。

## 系统观察

本轮 `vmstat` 在矩阵结束时多数采样的 CPU idle 为 97%--100%，I/O wait 为 0%--1%。
这说明这一次的短 64 MiB 本机矩阵没有稳定打满物理盘，主要展示的是 V2 单 EventLoop、
同步执行与数据复制的扩展边界；不能据此声称磁盘一定是当前唯一瓶颈。

较长的既有 1 GiB profile 在相同 V2 数据路径上出现 14%--37% I/O wait，且有大量
block I/O。它支持“长时间连续写会受到写回/磁盘影响”的判断，但与本次矩阵分开
记录，不能把两个样本拼成同一次测量。

## 火焰图与 CPU 证据

同一 V2 数据面之前已经以帧指针采集了 DataNode CPU profile：

- [DataNode 用户态火焰图](/tmp/minikv-v2-profile-frame-pointer-20260729-101825/v2-datanode-cpu.svg)
- `perf.data`：`/tmp/minikv-v2-profile-frame-pointer-20260729-101825/perf.data`
- `perf report`：`/tmp/minikv-v2-profile-frame-pointer-20260729-101825/perf-report.txt`

本次矩阵无法重新采样：宿主机的 `kernel.perf_event_paranoid=4`，当前账号没有
`CAP_PERFMON`；尝试录制 `cpu-clock:u` 被内核拒绝。上述火焰图不是本次矩阵重新生成，
但它来自同一 V2 代码路径，作为代码级热点证据使用。

既有 profile 的可验证观察：

- `__memmove_avx_unaligned_erms` 自身约 6.49%。调用链含 `TcpConnection::send`、
  HTTP body 处理与副本转发。
- `pwrite` 的调用链为 `FastDataStore::WriteSession::append` ->
  `ChunkUploadStream::consume`，即 I/O loop 内直接写盘。
- `pread` 的调用链为 `FastDataStore::get`，下载先把整个 Chunk 读入 `std::string`。
- `SHA256_Update` 出现在写入和读取校验路径中；这是内容寻址正确性所需的工作，不应
  被误判为无意义开销。

## 对应代码位置

1. 入站 64 KiB body 同时 hash、`pwrite` 与转发：
   [datanode_main.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/datanode_main.cpp:174)，
   [FastDataStore.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/FastDataStore.cpp:43)。
2. 副本连接未就绪/拥塞时的有限排队与 pause：
   [ReplicaUploadPipe.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/ReplicaUploadPipe.cpp:87)。
3. 出站 HTTP 写把 body 交给 `TcpConnection`：
   [AsyncHttpClient.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/http/AsyncHttpClient.cpp:121)。
4. 当前 `send(const char*, size_t)` 会构造 `std::string`，跨 EventLoop 再捕获一份：
   [TcpConnection.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/network/TcpConnection.cpp:200)。
5. 下载路径 `pread` 到完整 `std::string` 后重新 SHA-256：
   [FastDataStore.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/FastDataStore.cpp:155)。
6. HTTP streaming parser 每次最多向业务交付 `maxStreamBodyBytes_`，当前为有限窗口而
   非整块组装：
   [HttpContext.hpp](/home/ubuntu/miniKV/v1.0/miniKVCine/include/http/HttpContext.hpp:237)。

## 结论与 V4 优先级

V2 的正确边界已经成立：直传、链式副本、背压、持久物理索引、commit、下载校验均在
本机矩阵中完成并通过。它是“可用数据面”，不是线性扩展的高并发存储引擎。

按收益和风险排序，V4 建议为：

1. **先测再拆分磁盘路径**：将 `pwrite`、大块 `pread` 与校验移动到有界 disk worker
   pool；I/O loop 只做 epoll、协议状态和背压决策。
2. **多 I/O EventLoop**：连接固定归属一个 loop，避免一个慢磁盘/副本拖住所有连接。
3. **降低应用层复制**：让异步发送接收拥有可移动的数据块，使用 iovec/writev 或块
   引用队列；不能因为仍需 hash 就宣称上传可以完全零拷贝。
4. **下载流式/零拷贝路径**：已有 `TcpConnection::sendfile` 基础，但当前 Chunk 仍经
   `FastDataStore::get` 完整读入字符串。对已落盘、无需服务端重算 hash 的下载和 repair，
   应改为 extent + `sendfile`；客户端按 manifest hash 校验。
5. **受控客户端并发窗口**：本次 4 worker 的尾部明显恶化，后续应以节点队列、RTT、
   写盘延迟动态限制并发，而不是无上限增大并发数。

## 复现

```bash
cmake -S . -B build-perf -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build-perf -j$(nproc)

export MINIKV_V2_BIN_DIR="$PWD/build-perf/bin"
export MINIKV_V2_CLUSTER_SECRET='local-benchmark-secret'
tools/start_v2_local_benchmark_cluster.sh

build-perf/bin/minikv_v2_bench local --gateway-host 127.0.0.1 --gateway-port 18181 \
  --output-dir /tmp/v2-bench --sizes 64MiB --runs 3 --concurrency 2

tools/stop_v2_local_benchmark_cluster.sh
```

若要重新采集火焰图，宿主机管理员需要允许当前用户采样，例如临时设置
`kernel.perf_event_paranoid=1` 或授予受控的 `CAP_PERFMON`；完成后应恢复系统策略。
