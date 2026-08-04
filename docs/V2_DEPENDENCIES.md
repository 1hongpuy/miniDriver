# V2 运行依赖

## D2 Redis Streams

Gateway 的后台媒体任务 Publisher 使用 `hiredis`，Redis 服务端部署在配置指定的机器。

Ubuntu/Debian：

```bash
sudo apt update
sudo apt install redis-server libhiredis-dev
```

`minikv_v2_gateway` 不在 HTTP EventLoop 中访问 Redis。它只将已写入 Gateway LevelDB 的
任务放入进程内有界队列，由单独 Publisher 线程使用 hiredis 调用 `XADD`。Redis 断开或
Publisher 队列暂满时，任务仍保留为 `PENDING`，由 D2 的补偿扫描重新投递。
