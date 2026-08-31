# miniDriver V3-Lite Phase 0 基线记录（2026-08-18）

## 状态

Phase 0 已建立，V2 数据面未修改。

| 项目 | 结果 |
|---|---|
| Git 基线 | `v2.0.0` |
| V3 分支 | `v3` 已创建 |
| 构建 | `cmake --build build -j2` 通过 |
| CTest | 47/48 通过 |
| 已知失败 | `v2_thumbnail_contact_sheet_ui` 缺少 Python `playwright` |
| 数据目录 | V3 使用独立 `/tmp/minidriver-v3-lite`，不复用 V2 目录 |

## CTest 结果解释

完整本机网络权限下，48 项测试中 47 项通过。唯一失败为环境依赖：

```text
ModuleNotFoundError: No module named 'playwright'
```

第一次在受限沙箱内运行时，loopback/Redis 测试还会出现 `Operation not permitted`；在完整本机权限下，
这些数据面和控制面回归均通过。因此该失败不作为 V3 代码回归。

## Phase 0 产物

- `configs/v3-lite.local.yaml`：单 Gateway、三 Metadata、三 DataNode 的本地拓扑契约；
- `include/control/FaultInjection.hpp`：时钟、随机数和故障注入接口骨架；
- `tools/prepare_v3_lite_local.sh`：创建独立 V3 运行目录；
- `tools/start_v3_lite_local.sh` / `tools/stop_v3_lite_local.sh`：Metadata 进程启动契约；
- `docs/V3_TASK.md`：V3-Lite 任务、设计和验收标准；
- `docs/V3_CONTROL_PLANE_HA_IMPLEMENTATION_PLAN_2026-08-17.md`：完整 HA 规划。

## 执行命令

```bash
cmake --build build -j2
ctest --test-dir build --output-on-failure
tools/prepare_v3_lite_local.sh
```

Phase 0 不启动 Metadata/Raft 进程；它们将在 Phase 2 接入成熟 Raft 库后加入构建目标。
