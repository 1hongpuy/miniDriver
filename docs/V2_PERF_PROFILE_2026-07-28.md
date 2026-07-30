# V2 DataNode 性能采样记录

## 1. 采样目的与边界

本次不是网络带宽压测，而是先在同一台 Gateway 上启动两个独立 DataNode 进程，
用 `127.0.0.1` 跑一轮 1 GiB 上传、链式副本和下载。目的只有一个：把 V2 数据面
当前的 CPU、复制和磁盘路径定位出来，避免先猜测瓶颈。

因此结果不能代表 Tailscale 跨机器吞吐；它代表 V2 当前应用层数据路径的上限和开销。

## 2. 产物位置

本轮采样文件放在临时目录，避免提交 `perf.data` 等运行产物：

```text
/tmp/minikv-v2-profile-cpu-20260728-220707/
  perf.data              perf 原始采样
  perf.script            调用栈文本
  perf.folded            FlameGraph 折叠栈
  v2-datanode-cpu.svg    可在浏览器打开的火焰图
  perf-report.txt        perf report 文本报告
  perf-stat.txt          task-clock / context switch / fault 统计
  vmstat.txt             采样期间系统 I/O 和 CPU 状态
  benchmark/summary.csv  端到端吞吐结果
```

直接从本机打开 SVG：

```bash
xdg-open /tmp/minikv-v2-profile-cpu-20260728-220707/v2-datanode-cpu.svg
```

若在无桌面服务器上，可将该文件复制到本地电脑后用浏览器打开。

## 3. 本轮结果

```text
对象大小：1 GiB
上传并完成链式副本：2.805 s，约 365 MiB/s
下载并校验：11.148 s，约 91.8 MiB/s

两个 DataNode 合计：6.818 s task-clock / 25 s 观察窗口
上下文切换：约 2.5K / s
```

采样时 `vmstat` 可见写出约 300 MiB/s，部分时间窗口 I/O wait 为 16% 到 18%。
这说明本轮不是纯 CPU 测试，磁盘和页缓存也已经参与限制。

## 4. 火焰图如何读

横向宽度代表该调用栈占用的 CPU 样本比例，纵向代表调用深度。底部是入口，上方是被
调用函数。火焰图中的宽条不一定意味着函数自身慢，也可能代表它的子调用耗时长；因此
需要同时看 `perf report` 的 self overhead。

本次最明确的热点是：

```text
__memmove_avx_unaligned_erms  self 9.33%
```

调用栈可追到：

```text
ChunkUploadStream::consume
  -> ReplicaUploadPipe::push
  -> AsyncHttpRequest::writeInLoop
  -> TcpConnection::send(std::string const&)
  -> TcpConnection::sendInLoop
  -> Buffer::append
  -> memmove / string copy
```

这不是协议错误，而是当前 V2 为了线程安全和有界输出缓冲接受的用户态复制成本。它是
V4 优化的第一类目标：减少 `std::string` 中间对象、使发送路径接收可转移的缓冲块，并
在下载路径使用 `pread/sendfile`。

第二条确认的调用栈是：

```text
SHA256_Update
  -> FastDataStore::WriteSession::append
  -> ChunkUploadStream::consume
  -> HttpContext::streamBody
  -> HttpServer::onMessage
  -> TcpConnection::handleRead
  -> EventLoop::loop
```

因此 V2 的 SHA-256 与 `pwrite` 仍直接发生在 DataNode 的单个 EventLoop 回调中。它
保证了现在的逻辑简单和顺序性，但慢磁盘、哈希或高并发连接会延迟同一 EventLoop 的其他
连接。这是 V4 的 disk worker pool 和 multi-reactor 要解决的边界，不应在 V2 中暗中
变成无界线程池。

下载低于上传的原因也符合当前实现：`FastDataStore::get` 先把整个 Chunk 读入
`std::string`，然后 `HttpResponse::appendToBuffer` 和 `TcpConnection::send` 再复制到
网络输出缓冲。对应位置：

```text
src/DataNode/FastDataStore.cpp:155
include/http/HttpResponse.hpp:70
src/network/TcpConnection.cpp:200
```

## 5. 当前可下的结论

1. V2 的流式上传和链式副本已经工作：上传过程中不会将完整 4 MiB Chunk 组装为一个
   HTTP body 字符串。
2. 它仍不是零拷贝：副本发送、HTTP response 和下载均存在用户态复制，采样已经证明
   `memmove` 是显著热点。
3. 单 EventLoop 承担 HTTP 解析、SHA-256、`pwrite` 和副本转发；在多上传连接下，它是
   最需要监控的执行边界。
4. 本轮只有本机 loopback。下一步若要评估三台机器，应在客户端机器运行同一 benchmark，
   再结合 Tailscale RTT、节点出口和磁盘 I/O 解释结果。

## 6. 复跑命令

先启动隔离集群：

```bash
export MINIKV_V2_CLUSTER_SECRET="$(cat ~/minikv-v2-secrets/cluster.secret)"
./tools/start_v2_local_benchmark_cluster.sh
```

记录两个 DataNode 的 PID 后，以软件 CPU 时钟采样：

```bash
perf record -e cpu-clock -F 199 --call-graph dwarf,8192 \
  -p <datanode-a-pid>,<datanode-c-pid> \
  -o /tmp/perf.data -- sleep 30

perf script -i /tmp/perf.data > /tmp/perf.script
/tmp/FlameGraph/stackcollapse-perf.pl /tmp/perf.script > /tmp/perf.folded
/tmp/FlameGraph/flamegraph.pl --title "MiniKVCine V2" /tmp/perf.folded > /tmp/v2.svg
```

不要优先使用虚拟机里的 `cycles`。本环境的硬件 PMU 计数不可靠；`cpu-clock` 虽然是软件
事件，但足以定位 CPU 栈热点。
