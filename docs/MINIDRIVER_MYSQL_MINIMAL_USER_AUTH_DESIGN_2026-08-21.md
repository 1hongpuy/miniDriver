# MiniDrive：MySQL 最小用户与私有对象设计（2026-08-21）

## 1. 结论与范围

本设计为 MiniDrive V2 增加一条最小但完整的用户链路：

```text
注册 / 登录
  → Gateway 确认当前用户
  → 用户只能看到、上传、下载和删除自己的对象
  → DataNode 仍只接收短期、受限的能力凭证
```

目标不是做账号中心、RBAC、计费系统或高性能鉴权服务，而是让当前固定的 `admin` 文件空间变为多个彼此隔离的
个人空间。MySQL 只保存**账号与登录会话**；文件、Chunk、副本、上传会话、目录和对象元数据仍由现有
`GatewayState + LevelDB` 保存。

这一边界很重要：不能为了加入 MySQL，把每一个 64 KiB Block、Chunk 写入或 DataNode 请求都改成查询 MySQL。
那既会破坏 V2 数据面，也不能带来更正确的权限模型。

本期建议命名为 **User Auth Lite / U0**，交付后可准确表述为：

> MiniDrive 已实现基于 MySQL 的用户注册、密码哈希登录、服务端会话与对象所有者隔离；Gateway 在控制面鉴权，
> DataNode 使用由 Gateway 签发的短期上传/下载能力凭证，不持有用户密码或会话。

不应表述为“完整多租户 IAM”“零信任安全存储”或“生产级权限系统”。

## 2. 当前代码基础与真实缺口

现有存储模型其实已预留所有者字段：

```cpp
SessionState::ownerId
FileMeta::ownerId
DirectoryMeta::ownerId
ObjectMeta::ownerId
```

且目录 LevelDB key 已按 `ownerId + path` 构造。因此不需要迁移 Chunk 格式，也不需要将对象元数据搬进 MySQL。

但当前 Gateway HTTP 路由没有用户认证；若干 `GatewayState` 方法仍固定以 `"admin"` 查找目录或列举对象。任何
访问 Gateway 的浏览器都等价于使用同一个 `admin` 空间。

此外，当前 DataNode 的公开读路径如下：

```text
GET /v2/chunks/{chunkHash}
```

它目前只做 CORS 和下载资源准入，不验证用户身份或对象所有权。也就是说，**只保护 Gateway 的目录和 manifest
接口并不足以称为私有文件系统**：获得 DataNode 地址和 Chunk hash 的人仍可能读取数据。

U0 因而有两条必须同时完成的最小约束：

1. Gateway 将已认证用户传入对象/目录/上传会话的 owner 检查；
2. Gateway 对浏览器下载签发短期、逐 Chunk 的下载 capability，DataNode 校验后才返回 Chunk。

## 3. 推荐部署：MySQL 跟随 Gateway，而不是 AI 数据库

当前三主机中，用户认证最合理的最小部署是：

```text
gateway 100.75.93.124
  ├─ MiniDrive Gateway :18081
  └─ MySQL :3306（仅绑定 127.0.0.1 或 Docker 内部网络）

node-d 100.75.72.15
  └─ DataNode（不连接 MySQL）

ubuntu22data1 100.89.50.125
  └─ CineLake Redis / PostgreSQL / AI Worker（不连接 MySQL）
```

理由：用户会话只在 Gateway 控制请求上使用；将 MySQL 放在 AI 主机只是额外增加一次跨机登录依赖，也混淆
PostgreSQL/pgvector 的 AI 元数据职责和 MiniDrive 用户身份职责。

开发环境可用 Docker Compose 起一个 MySQL 容器和命名 volume；MySQL 端口不映射到公网，也不需要向 DataNode、
浏览器或 AI Worker 开放。Gateway 用专属的低权限数据库账号连接 `127.0.0.1:3306`。

```text
Browser ── Cookie ──> Gateway ── localhost TCP ──> MySQL
Browser ── short HMAC capability ──> DataNode
DataNode ── internal commit/heartbeat ──> Gateway
```

这里的第二条不是“浏览器登录 DataNode”。Cookie 永远只给 Gateway；DataNode 只验证 Gateway 用集群密钥签发的、
有效期很短且范围明确的能力凭证。

> V3 提示：三 Gateway 后，账号/会话状态不能各自使用独立 MySQL。V3 应把用户身份服务明确为独立共享依赖，
> 或将会话/权限版本纳入一致性控制面。本报告只解决 V2 单 Gateway 的最小闭环。

## 4. 职责划分

| 模块 | 保存或处理什么 | 不处理什么 |
|---|---|---|
| MySQL | 用户、Argon2id 密码哈希、会话 token 哈希、禁用状态 | 文件 Body、Chunk、副本、向量、AI Asset |
| Gateway + LevelDB | ownerId、目录、对象、上传 Session、路由、commit 状态 | 明文密码、原始 session token |
| Gateway AuthExecutor | 密码校验、MySQL 查询、会话创建/撤销 | Reactor 线程内阻塞 I/O |
| DataNode | 校验 HMAC upload/download capability，读写本地 Chunk | MySQL 连接、Cookie、用户名、密码 |
| 前端 | 登录、登出、显示当前用户；同源 Cookie 自动附带 | localStorage 保存密码或长期 token |
| CineLake AI | 继续根据受控 Worker 接口读取已 commit 对象 | 认证数据库或浏览器登录代理 |

## 5. MySQL 最小数据模型

用户名在 U0 只支持 ASCII 的 `a-z`、`A-Z`、数字、`_`、`-`、`.`，长度 3～64。这是为了让登录名、日志和
未来 ownerId 映射没有 Unicode 大小写/归一化歧义；显示昵称可以以后单独加入。

`user_id` 使用随机 UUID 字符串，作为 LevelDB 中的 `ownerId`；**不要使用 username 作为所有者主键**，否则改名
会变成一次危险的全量元数据迁移。

```sql
CREATE DATABASE IF NOT EXISTS minidrive_auth
  CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

CREATE USER 'minidrive_gateway'@'localhost' IDENTIFIED BY '<仅写入私有 .env 的强密码>';
GRANT SELECT, INSERT, UPDATE, DELETE ON minidrive_auth.* TO 'minidrive_gateway'@'localhost';

USE minidrive_auth;

CREATE TABLE users (
  user_id       CHAR(36)     NOT NULL,
  username      VARCHAR(64)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  status        ENUM('active', 'disabled') NOT NULL DEFAULT 'active',
  created_at    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                              ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (user_id),
  UNIQUE KEY uk_users_username (username)
) ENGINE=InnoDB;

CREATE TABLE user_sessions (
  token_hash  BINARY(32) NOT NULL,
  user_id     CHAR(36)   NOT NULL,
  created_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  expires_at  DATETIME(6) NOT NULL,
  revoked_at  DATETIME(6) NULL,
  last_seen_at DATETIME(6) NULL,
  PRIMARY KEY (token_hash),
  KEY idx_sessions_user_expiry (user_id, expires_at),
  CONSTRAINT fk_sessions_user FOREIGN KEY (user_id) REFERENCES users(user_id)
    ON DELETE CASCADE
) ENGINE=InnoDB;
```

### 5.1 密码与 Token 的存储规则

```text
password       → Argon2id(password, random salt, memory/time/parallelism 参数) → password_hash
session token  → 32 random bytes → base64url（仅作为 Cookie 返回）
cookie token   → SHA-256(token bytes) → token_hash（仅此摘要写 MySQL）
```

- 密码绝不能用 SHA-256、MD5、明文或可逆加密保存；推荐接入 libsodium 的 Argon2id。bcrypt 可以作为受限环境的
  兼容方案，但不能自创密码哈希；
- `SHA-256(token)` 用于服务器端保存**高熵随机 token 的校验值**是合适的，它不是密码哈希替代品；
- 原始 token 只存在于 TLS/HTTP 响应和浏览器 HttpOnly Cookie 中，数据库泄漏后攻击者不能直接拿表中摘要登录；
- 不在 C++ 日志、异常、URL query 或 Redis event 中记录 password、Cookie 或原始 token。

## 6. 登录与会话语义

U0 推荐 opaque server-side session，而不是 JWT。

JWT 看上去少一次数据库查询，但提前引入密钥轮换、撤销、用户禁用后失效、多个 Gateway 的缓存一致性等问题。
当前只需要一个 Gateway，opaque session 的退出和禁用语义更直接。

### 6.1 HTTP API

```text
POST /api/v2/auth/register
  { "username": "hpy", "password": "..." }
  → 201 { "user": { "id": "uuid", "username": "hpy" } }

POST /api/v2/auth/login
  { "username": "hpy", "password": "..." }
  → 200 + Set-Cookie: md_session=<opaque token>; HttpOnly; SameSite=Lax; Path=/
  → { "user": { "id": "uuid", "username": "hpy" }, "expiresAt": "..." }

POST /api/v2/auth/logout
  → 将 token_hash 写为 revoked_at；Set-Cookie: md_session=; Max-Age=0; Path=/

GET /api/v2/auth/me
  → 200 { "user": { "id": "uuid", "username": "hpy" } }
  → 401（未登录、过期或已撤销）
```

登录失败统一返回 `401 invalid username or password`，不区分“用户不存在”和“密码错误”。用户名冲突可在注册时返回
`409 username already exists`。

Cookie 在启用 HTTPS 时必须包含 `Secure`；本机 HTTP 调试仅可通过明确的
`MINIDRIVE_AUTH_COOKIE_SECURE=false` 配置关闭，默认生产配置必须为 `true`。前端与 Gateway 同源部署时，浏览器
对 `/api/v2/*` 的 fetch 会自动携带 Cookie；不应把 session token 放入 localStorage。

### 6.2 请求认证流程

```text
Browser → GET /api/v2/catalog (Cookie: md_session=...)
        → Gateway 解析 Cookie
        → AuthService::authenticate(token)
        → AuthExecutor 查询 user_sessions + users
        → RequestPrincipal{ userId, username }
        → GatewayState 按 userId 查询/修改对象
```

即使 U0 不追求性能，MySQL 网络 I/O 和 Argon2id 也不能在 Multi-Reactor 的 EventLoop 线程中同步执行，否则一次慢
数据库连接或密码校验会卡住该 loop 上的其他 HTTP 连接。

最小实现增加一个有界 `AuthExecutor`（可先 1～2 个工作线程、64 个待处理任务）：

```text
Gateway EventLoop
  → 解析/校验 JSON、Cookie
  → 投递 AuthExecutor
  → MySQL 查询或 Argon2id 校验
  → queueInLoop 回原 HTTP connection 所属 EventLoop 响应
```

队列满时返回 `503 + Retry-After`，连接/查询超时返回 `503 auth service unavailable`。HTTP 请求超时只取消等待响应；
若注册或登录操作已经成功，客户端以相同请求幂等键重试时应得到可解释结果。U0 可先不缓存会话，以获得立即登出
和禁用；确认正确性后才加入最长 30～60 秒的正向会话缓存。

## 7. 从“admin”改为真实 owner 的对象链路

### 7.1 GatewayState 改造原则

当前 `GatewayState` 内已有 `ownerId`，但公开方法没有把它作为参数。U0 将用户身份只以显式 `ownerId` 传入，
不通过全局变量或 HTTP Header 偷传：

```cpp
// 示意接口，具体命名可按现有风格调整。
PreflightStatus preflightUpload(const std::string& ownerId,
                                const UploadPreflightRequest&, UploadPreflightResult&);
bool createSession(const std::string& ownerId, ... , SessionState&);
bool getSessionForOwner(const std::string& ownerId, const std::string& sessionId,
                        SessionState&);
RoutePlanStatus planRoutesForOwner(const std::string& ownerId, ...);
FileCommitStatus commitFileForOwner(const std::string& ownerId, ...);
bool createDirectory(const std::string& ownerId, const std::string& parentPath, ...);
bool listCatalog(const std::string& ownerId, const std::string& path, CatalogSnapshot&);
bool getObjectForOwner(const std::string& ownerId, const std::string& objectId, ObjectMeta&);
DeleteStatus deleteObjectForOwner(const std::string& ownerId, const std::string& objectId);
```

每一个进入上传 Session、对象或目录的外部请求都要比较 `principal.userId == record.ownerId`；不匹配时优先返回
`404`，而不是暴露某个 objectId/sessionId 是否存在。文件 commit 继续从已拥有 ownerId 的 `SessionState` 写入
`FileMeta/ObjectMeta`，不接受客户端提交的 `ownerId`。

用户可用的路径因此为：

```text
登录 principal.userId
  → preflight(ownerId) 创建 Session(ownerId)
  → routes 前检查 Session.ownerId
  → commit 从 Session 继承 ownerId
  → ObjectMeta/DirectoryMeta 用 ownerId 作为 Catalog namespace
  → catalog/get/delete 再次按 ownerId 校验
```

内部 DataNode 的 chunk commit、heartbeat、媒体 Worker 派生上传继续使用 Cluster Internal Token 或已有 job lease，
不能要求它们携带浏览器 Cookie。派生对象必须从源对象/媒体 Job 继承 ownerId，不能默认落回 `admin`。

### 7.2 下载 capability：私有对象的必要补丁

上传已经使用 HMAC `UploadCapability`，它适合让 DataNode 在不查 MySQL 的情况下验证某个临时 Chunk 写入。下载
采用同样思路，但不能复用 upload token：用途、方法、过期时间和可访问范围必须分开，防止权限混淆。

```text
Browser
  → GET /api/v2/objects/{objectId}/manifest（Cookie）
  → Gateway 确认 object.ownerId == principal.userId
  → 对 manifest 中每个 chunk 签发 60 秒 DownloadCapability
  ← node endpoint + chunkHash + downloadToken

Browser
  → GET DataNode /v2/chunks/{chunkHash}
       Header: X-MiniDrive-Download-Capability: <HMAC token>
  → DataNode 校验 method=GET、nodeId、chunkHash、objectId、expiresAt、nonce
  → sendfile 返回该 Chunk
```

建议 `DownloadCapability` 的已签名 payload 至少包括：

```text
version / purpose=download / objectId / objectVersion / chunkHash
nodeId / expiresAt / nonce
```

DataNode 检查本机 `nodeId` 与 token 一致、当前时间未超过 `expiresAt`、请求方法是 `GET/HEAD`、路径 hash 完全一致。
Capability 不包含 password、Cookie 或长期用户 token。对于 16 MiB/4 MiB Chunk 的普通对象，一个 manifest 只会产生
少量短 token；这符合 U0 的简化范围。

在下载 token 交付前，系统只能称为“Gateway 目录隔离”，不能称为“对象字节已隔离”。在此期间，DataNode
`/v2/chunks/*` 必须限制在可信内网，不能开放公网。

## 8. 路由权限矩阵

| 路由类型 | 是否需要浏览器会话 | 额外校验 |
|---|---:|---|
| `POST /api/v2/auth/register/login` | 否 | 输入格式、速率限制、密码 hash |
| `POST /api/v2/auth/logout`、`GET /auth/me` | 是 | session 有效、用户 active |
| `GET /catalog`、目录创建/删除 | 是 | ownerId namespace |
| upload preflight/session/routes/commit | 是 | Session.ownerId 与 principal 相同 |
| `GET/DELETE /objects/{id}`、manifest | 是 | ObjectMeta.ownerId 与 principal 相同 |
| DataNode `PUT /v2/chunks/*` | 否 | 既有 UploadCapability + route/lease |
| DataNode `GET/HEAD /v2/chunks/*` | 否 | 新 DownloadCapability + 资源准入 |
| `/internal/v2/*` | 否 | Cluster Internal Token / job lease，绝不接受 Cookie 替代 |
| AI Worker 的 manifest/对象读取 | 否 | 保持独立 Worker 内部凭证；后续按 owner/服务账号收敛 |

`/api/v2/admin/nodes` 在 U0 不应向普通登录用户开放。最简单选择是只允许配置文件中固定的
`MINIDRIVE_ADMIN_USER_IDS`；若尚未实现角色字段，则暂时隐藏该路由或仅允许 internal token，不要把“已登录”
误当作“管理员”。

## 9. API 与前端最小体验

前端保持同源：`www-v2` 由 Gateway 提供，登录后再加载/操作 Catalog。最小 UI 不需要注册中心页面或复杂个人资料：

```text
未登录：用户名、密码、登录按钮、注册链接
已登录：显示 username、退出按钮、原有文件工作台
会话过期：停止当前操作，显示“登录已过期”，回到登录页
```

上传断点续传的 localStorage key 需要带 `userId` 前缀：

```text
upload-resume:{userId}:{manifestHash}
```

否则浏览器在 A 用户上传到一半、退出并让 B 用户登录时，B 可能尝试恢复 A 的 Session。Gateway 的 owner 校验会拒绝它，
但前端也应主动清理或隔离该本地状态，避免给用户造成“恢复失败”的困惑。

对现有 `admin` 数据建议定义一次性兼容策略：

```text
legacy ownerId = "admin"
初始创建一个管理用户，其 user_id 由配置 MINIDRIVE_LEGACY_ADMIN_USER_ID 指定
该 user_id 与历史 LevelDB ownerId 对齐，或执行一次明确的 owner backfill
普通新用户永远不可见 legacy admin 对象
```

不能在第一次登录时把历史对象“自动送给第一个注册用户”；这既不可审计，也可能泄露文件。若历史 `ownerId` 就是
字符串 `admin`，最小迁移可以保留它作为一个专门 legacy principal，并在配置中将该 principal 映射到唯一管理员。

## 10. C++ 模块、配置与依赖建议

新增层保持小并可替换：

```text
include/auth/
  AuthTypes.hpp             # User, RequestPrincipal, SessionRecord
  PasswordHasher.hpp        # Argon2id 抽象
  IUserRepository.hpp       # MySQL 可替换为 Fake repository
  MySqlUserRepository.hpp
  AuthService.hpp
  AuthExecutor.hpp

src/auth/
  PasswordHasher.cpp
  MySqlUserRepository.cpp
  AuthService.cpp
  AuthExecutor.cpp

db/mysql/
  001_auth_schema.sql
```

建议使用 **MariaDB Connector/C 或 MySQL Connector/C 的 C API + prepared statement**，并用 RAII 包装
`MYSQL*`、`MYSQL_STMT*`、结果集和事务。原因是当前项目是 C++17/CMake，Connector/C 的接口小、部署清晰，足够满足
U0；不应让 HTTP Handler 拼接 SQL 字符串。

配置只放私有 YAML / `.env`，不进入 Git：

```yaml
auth:
  enabled: true
  mysql:
    host: 127.0.0.1
    port: 3306
    database: minidrive_auth
    user: minidrive_gateway
    passwordFile: /etc/minidriver/auth-db.password
  session:
    ttlSeconds: 604800
    cookieName: md_session
    secureCookie: true
  executor:
    threads: 2
    maxPending: 64
```

MySQL 连接、密码文件、Argon2 参数及 cookie secure 开关均需在启动时验证；缺失或不可连接时，启用了
`auth.enabled` 的 Gateway 应明确启动失败，而不是悄悄退化为 anonymous/admin。

## 11. 分阶段实施计划与验收

### U0-0：冻结边界与本地 MySQL

- 添加 MySQL Compose profile/独立 Gateway 运维文件与私有 `.env.example`；
- 添加 `001_auth_schema.sql`，以专用低权限账号创建表；
- 不修改 DataNode 数据文件或 AI PostgreSQL。

验收：MySQL volume 重启后用户表仍存在；Gateway 无 auth 配置时仍按当前 V2 行为启动；开启 auth 但连接失败时明确失败。

### U0-1：认证核心

- 实现 `IUserRepository`、Argon2id、随机 session token、token hash、AuthExecutor；
- 实现 register/login/logout/me 和统一 `RequestPrincipal`；
- 增加最小的按 IP 内存登录失败限制，例如 5 次/15 分钟返回 `429`。

验收：密码不出现在日志/MySQL 明文；错误密码与未知用户得到相同 401；logout/disabled user 立即不能通过 `/me`。

### U0-2：Gateway owner 隔离

- 为 Catalog、目录、预检、Session、route、commit、object get/delete 的 `GatewayState` API 补 `ownerId`；
- 消除内部固定 `"admin"` 的普通用户路径；
- 为 legacy `admin` 数据执行显式映射或 backfill；
- 给上传恢复 localStorage 加 userId namespace。

验收：用户 A 上传/创建目录后，用户 B 的 catalog 不出现它；B 访问 A 的 objectId/sessionId/目录路径返回 404；
A 仍可断点续传并完成 commit。

### U0-3：DataNode 下载凭证

- 定义与 upload capability 分离的 `DownloadCapability`；
- Gateway 只在 owner 校验通过后把 token 放入 manifest；
- DataNode 在 GET/HEAD 前验证 token，再进入现有 download admission 和 `sendfile` 路径；
- 更新浏览器下载/预览请求携带 header。

验收：无 token、过期 token、错误 hash/node/method 的 DataNode GET 均为 403；合法用户可完整下载且 SHA-256 与原文件一致；
上传 capability 不能用于下载，下载 capability 不能用于上传。

### U0-4：最小前端与回归

- 加登录/退出状态和 session 过期处理；
- 加启动、迁移、创建 legacy admin、备份 MySQL volume 的运维文档；
- 执行构建、认证集成、双用户隔离、上传/下载 SHA-256、原 V2 回归。

验收证据至少保存：SQL migration 版本、测试用户 ID（非密码）、HTTP 状态码、Gateway/DataNode 审计日志、对象
SHA-256 和已知限制。

## 12. 测试清单

| 类别 | 最小测试 |
|---|---|
| 密码 | Argon2id 哈希/验证；错误密码拒绝；日志脱敏 |
| 会话 | 登录、`/me`、登出、过期、禁用用户、随机 token 不重复 |
| SQL | prepared statement；用户名唯一；session token 只存 hash；迁移可重复执行 |
| 隔离 | A/B 目录、对象、Session、preflight、delete 均不能跨 owner |
| 数据面 | 无/错/过期下载 capability 被 DataNode 拒绝；正确 token 下载 SHA-256 正确 |
| 回归 | 原单 admin 数据可由 legacy admin 读取；上传双副本、Redis AI outbox、缩略图链路不退化 |
| 失败 | MySQL 不可用、AuthExecutor 满、连接超时分别有明确 503 和日志，不阻塞 EventLoop |

测试中使用独立临时 MySQL schema 或 Compose volume；不得对现有运行中的 AI PostgreSQL/pgvector volume 或
`data/` 运行目录执行清空操作。

## 13. 明确不做的内容

U0 有意不实现：

- OAuth、短信/邮件验证码、找回密码、多因素认证；
- 多租户组织、RBAC/ACL、分享链接、配额和计费；
- JWT、跨 Gateway session 缓存、MySQL 主从或数据库高可用；
- 用户级加密、静态数据加密、审计平台和 WAF；
- 让 MySQL 参与 Chunk 写入、DataNode heartbeat、Redis AI 事件或向量检索。

这些都可以在“账号正确、owner 隔离正确、私有下载凭证正确”之后再迭代。对当前 MiniDrive，正确的最小顺序是：

```text
身份确认 → owner 隔离 → 受限 DataNode 下载 → 前端体验 → 再考虑角色/分享/配额
```

