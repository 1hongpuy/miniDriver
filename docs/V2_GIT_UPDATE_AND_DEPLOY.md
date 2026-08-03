# V2 Git 更新与多机部署流程
本文用于把 V2 代码从开发机推送到 GitHub，再安全更新 Gateway、Node A、Node C。

当前约定：

- Git 分支：`v2`
- Gateway 控制面：`100.75.93.124:18081`
- DataNode 数据面：每台节点的 `:9002`
- 浏览器前端：`http://100.75.93.124:8082`
- 进程启动器：`minikv_v2_node_agent`

不要把 `cluster.secret`、`~/minikv-v2/config/*.yaml`、运行日志、`run/` 数据目录加入 Git。

## 1. 开发机：检查并选择性提交

在仓库根目录执行：

```bash
cd ~/miniKV/v1.0/miniKVCine
git branch --show-current
git status --short
git diff --check
```

第一条命令必须输出 `v2`。提交前先阅读待提交文件，不要使用不加筛选的
`git add .`。

例如本轮 DataNode 延迟响应 CORS 修复只提交：

```bash
git add src/DataNode/datanode_main.cpp
git diff --cached --stat
git diff --cached
git commit -m "fix: add CORS headers to deferred chunk responses"
git push origin v2
```

如果同一轮还包含网络、Gateway、异步客户端修复，应明确列出对应文件：

```bash
git add \
  src/network/TcpConnection.cpp \
  include/http/AsyncHttpClient.hpp \
  include/gateway/GatewayState.hpp \
  src/gateway/GategayState.cpp \
  src/DataNode/ReplicaUploadPipe.cpp \
  src/DataNode/datanode_main.cpp

git diff --cached --check
git diff --cached --stat
git commit -m "fix: stabilize v2 upload data plane"
git push origin v2
```

提交后确认远端分支已同步：

```bash
git status
git log -1 --oneline
```

预期 `git status` 显示本地 `v2` 已与 `origin/v2` 同步。

## 2. 远端节点：无本地修改时拉取

以 Node C 为例：

```bash
cd ~/miniKV_v2/miniDriver
git fetch origin --prune
git switch v2
git pull --ff-only origin v2
git log -1 --oneline
git status
```

`--ff-only` 禁止 Git 在服务器上自动生成合并提交。远端节点只消费已经在开发机
审查和推送过的提交。

首次克隆 V2 分支：

```bash
git clone --branch v2 --single-branch \
  https://github.com/1hongpuy/miniDriver.git \
  ~/miniKV_v2/miniDriver
```

## 3. 远端节点：存在本地代码修改时拉取

如果 `git pull` 报错：

```text
Your local changes to the following files would be overwritten by merge
```

不要直接使用 `git restore` 或 `git reset --hard`。先把服务器上的旧修改安全暂存：

```bash
cd ~/miniKV_v2/miniDriver
git status

git stash push -m "node-c before pull v2" -- \
  include/gateway/GatewayState.hpp \
  include/http/AsyncHttpClient.hpp \
  src/gateway/GategayState.cpp \
  src/network/TcpConnection.cpp

git pull --ff-only origin v2
git status
git stash list
```

通常不应立刻执行 `git stash pop`：远端的新提交正是要替换这些旧代码。只有确认
服务器上存在独有的、需要保留的代码时才查看暂存差异：

```bash
git stash show -p stash@{0}
```

`~/minikv-v2/config/*.yaml`、日志、密钥和数据目录不在仓库内，执行 Git 拉取不会
覆盖它们。

## 4. 重新构建

每次拉取 C++、CMake 或头文件修改后，都在对应机器重新构建：

```bash
cd ~/miniKV_v2/miniDriver
cmake -S . -B build
cmake --build build -j"$(nproc)"
```

确认需要的二进制存在：

```bash
ls -l \
  build/bin/minikv_v2_node_agent \
  build/bin/minikv_v2_gateway \
  build/bin/minikv_v2_datanode
```

## 5. 正确停止 NodeAgent

NodeAgent 是父进程，会管理并在异常时重启 Gateway/DataNode。不要先单独杀子进程。

如果 Agent 在前台运行，直接按：

```text
Ctrl+C
```

如果误按了 `Ctrl+Z`，它只是被挂起，没有停止。应恢复后再停止：

```bash
jobs -l
fg %1
```

然后按 `Ctrl+C`。

确认进程已结束：

```bash
pgrep -af 'minikv_v2_(node_agent|gateway|datanode)'
```

没有输出才表示该机器的 V2 进程已完全停止。

## 6. 启动 Gateway / Node A

Gateway 机器的 YAML 通常同时管理 Gateway 与本机 DataNode：

```bash
cd ~/miniKV_v2/miniDriver

./build/bin/minikv_v2_node_agent \
  --config ~/minikv-v2/config/gateway.yaml \
  --bin-dir "$PWD/build/bin"
```

浏览器直连 DataNode 时，`gateway.yaml` 必须含有与浏览器地址完全一致的 CORS 来源：

```yaml
web:
  allowedOrigin: http://100.75.93.124:8082
```

`allowedOrigin` 是协议、主机和端口的组合，不带末尾 `/`。前端地址改变时，两台
DataNode 的该配置也必须同步更新，并重启 Agent。

## 6.1 异步结构化日志

每个 `services` 条目可单独配置异步日志。日志追加写入，达到 `rotateBytes` 后轮转；
日志队列满时丢弃最旧的诊断消息，不阻塞 EventLoop：

```yaml
logging:
  file: /home/ubuntu/minikv-v2/logs/datanode-node-a.log
  level: info
  queueSize: 8192
  rotateBytes: 20971520
  rotateFiles: 5
```

未设置 `logging` 时，默认路径为 `<dataDir>/logs/<service-id>.log`。Gateway 与
DataNode 必须使用不同的 `file`。NodeAgent 自己记录到配置文件同级的
`logs/node-agent.log`。

## 7. 启动 Node C

Node C 的 YAML 只管理它本机的 DataNode：

```bash
cd ~/miniKV_v2/miniDriver

./build/bin/minikv_v2_node_agent \
  --config ~/minikv-v2/config/node-c.yaml \
  --bin-dir "$PWD/build/bin"
```

Node C 同样需要：

- 与 Gateway 完全一致的 `cluster.secret`。
- 正确的 `advertiseAddress`，即 Node C 自己的 Tailscale IPv4 地址。
- `web.allowedOrigin: http://100.75.93.124:8082`。
- 到 Gateway `100.75.93.124:18081` 的 Tailscale 连通性。

## 8. 更新后的验证

Gateway 机器：

```bash
curl http://127.0.0.1:18081/api/v2/admin/nodes
tail -n 50 -F ~/minikv-v2/logs/gateway.log
```

Node A 和 Node C：

```bash
tail -n 50 -F ~/minikv-v2/logs/datanode-<node-id>.log
```

`-F` 比 `-f` 更适合调试：日志文件被重新创建时，它会自动重新打开。

一次两副本 Chunk 上传成功时，DataNode 日志最终应出现一条 `chunk_complete`：

```text
event=chunk_complete chunk=<sha256> session=<id> index=0 http_status=200 \
  total_ms=... local_write_ms=... replica_ms=... gateway_commit_ms=... \
  pauses=... pause_ms=... max_pending_bytes=...
```

用下面的命令查看慢路径：

```text
grep 'event=chunk_complete\|event=chunk_failed' ~/minikv-v2/logs/datanode-<node-id>.log
```

## 9. 前端单独部署

当前 V2 前端由 Nginx 从 `/opt/minikv-v2/www` 提供；它不在当前 Git 工作树的 `www/`
目录中。因此，后端 Git 更新不会自动更新该目录。

前端更新后应：

1. 确认 `/opt/minikv-v2/www/app.js` 的内容正确。
2. 浏览器使用 `Ctrl+Shift+R` 强制刷新，或用无痕窗口重新打开。
3. 以后应把 V2 前端源码纳入仓库的独立目录，再通过受控部署脚本复制到
   `/opt/minikv-v2/www`；不要只在运行目录手工修改且不保留源文件。
