# 技术选型

使用 ConnectRPC（一个兼容 gRPC 基于 Protobuf 的 RPC 协议）作为传输基础


# 设计核心原则

主要依靠 usn（update sequence number）字段和批量传输数据完成数据同步
同步颗粒度是 batch（知识单元数量待定），每一个 batch 对应一个数据库事务
服务器负责分配和递增 usn 字段，每条同步变更携带自己的 usn，batch 表示意味着一批变更，也是事务颗粒度。
collection.sync_cursor_usn = next_usn，同步游标，表示当前集合已同步到的 USN 上界，也是下次增量 Pull 的起点
unit.usn = last_modified_usn，表示实体最后一次被服务端确认的 USN，
本地未同步数据 unit.usn = -1


# 注意事项

## 设计检查清单：
1. 是否处理超时的情形
2. 是否处理超时后重连情形
3. 是否处理了传输中断数据不一致的问题
4. 是否服务器崩溃故障转移了


# 流程

## 流程简述

1. 双方握手
2. 根据握手状态客户端决定下一步：
  - NO_REMOTE_CHANGES：远端无新增。客户端若有本地 usn=-1，则 PUSH；否则结束。
  - NEED_PULL：远端有新增。客户端先 Pull；Pull 完成后若本地仍有 usn=-1，则 PUSH，否则结束。
  - OVERWRITE_REQUIRED：普通增量同步不安全，让用户选择覆盖方向，具体覆盖实现见“TODO覆盖同步流程”。
3. 开始批量传输数据
4. 数据传输完毕挥手

## 全局 Header

注意所有的 ConnectRPC 操作都带上了全局 header
 `Authorization: Bearer ${accessToken}`


## 握手

### 正常握手流程

(一个时序图，在飞书上)

### HandshakeRequest
HandshakeRequest 是发起握手该携带的数据

```proto
message HandshakeRequest {
  // 本地标识的设备 UUIDv7，用于区分同一用户的不同设备
  string device_id = 1 [(buf.validate.field).string.len = 36];
  // 设备展示名
  string device_name = 2 [(buf.validate.field).string.max_len = 32];
  // 集合 UUIDv7
  string collection_id = 3 [(buf.validate.field).string.len = 36];

  // 客户端 collection.sync_cursor_usn，已同步到的 USN 上界 / 下次增量 Pull 起点
  int64 client_sync_cursor_usn = 4 [(buf.validate.field).int64.gte = 0];

  // 同步协议版本
  int32 protocol_version = 5;
  // 客户端本地 SQLite schema 版本
  int32 db_schema_version = 6;
  // collection schema 变更时间，仅用于相等性比较，不用于判断哪一方更新
  int64 collection_schema_updated_at = 7;
}
```

device_id 用于区分同一用户的不同设备，并辅助判断 SyncLock 存在时是否为同一设备重复握手。
此外还会传集合元数据，以此确认目前服务器已有的数据是否有冲突问题（卡片模板变更，模板删除等）

### SyncLock
SyncLock 为 Redis 用户级服务端分布式锁，也起到维护 session 的作用
在验证 access_token 有效并解析出 user_id 后存一个分布式锁，除非 TTL 到了或者用户完成了后续同步操作后才会释放，目的是禁止同步期间用户用其他设备也同步。若再次握手时 SyncLock 仍存在，则根据 device_id 判断是否为同一设备恢复当前 session
此时的 TTL 为 1 分钟, key 为用户 sync:{userID}:sync_lock

```json
{
    "user_id": xxx, // 利用 userID 锁住用户其他设备的同步操作
    "state": xxx, // 处于同步的什么状态（阶段）
    // 是当前 state 内期望的下一个 batch 序号；进入 PULL 或 PUSH 状态时重置为 1。
    "expected_batch_seq": xxx,
    "session_id": xxx,
    "client_sync_cursor_usn_at_handshake": xxx,
    "server_sync_cursor_usn_at_handshake": xxx,
    "device_id": xxx
}
```

### HandshakeResponse
HandshakeResponse 表示此次握手结果。
握手结果有以下：

```proto
message HandshakeResponse {
  HandshakeStatus status = 1;

  // 服务端随机生成的 16 字节 session_id（转为字符串后长度为 32）
  // NO_REMOTE_CHANGES / NEED_PULL / OVERWRITE_REQUIRED 返回，其他状态不返回
  optional string session_id = 2 [(buf.validate.field).string.len = 32];

  int64 server_sync_cursor_usn = 3 [(buf.validate.field).int64.gte = 0];
}

enum HandshakeStatus {
  HANDSHAKE_STATUS_UNSPECIFIED = 0;

  // client_sync_cursor_usn == server_sync_cursor_usn
  HANDSHAKE_STATUS_NO_REMOTE_CHANGES = 1;
  // client_sync_cursor_usn < server_sync_cursor_usn
  HANDSHAKE_STATUS_NEED_PULL = 2;

  // 普通增量同步不安全，需要用户选择覆盖方向
  HANDSHAKE_STATUS_OVERWRITE_REQUIRED = 3;

  // 其他客户端正在进行同步
  HANDSHAKE_STATUS_LOCKED_BY_OTHER_CLIENT = 4;

  // 客户端 protocol_version 或 db_schema_version 低于服务端最低支持版本
  HANDSHAKE_STATUS_CLIENT_TOO_OLD = 5;
  // 客户端 protocol_version 高于服务端当前支持版本
  HANDSHAKE_STATUS_SERVER_TOO_OLD = 6;
}

```

session_id 由服务端在允许继续当前同步会话时生成，用于标识本次同步会话。

NO_REMOTE_CHANGES / NEED_PULL / OVERWRITE_REQUIRED 返回 session_id，其他状态下不返回 session_id。字段存在时必须是 32 位字符串，后续 Pull / Push / FinishSync 都需要携带 session_id。


#### NO_REMOTE_CHANGES
NO_REMOTE_CHANGES 表示 client_sync_cursor_usn == server_sync_cursor_usn，服务器相对客户端同步游标无新增数据。客户端下一步是否进入 PUSH 取决于本地是否存在 usn = -1。


#### NEED_PULL

client_sync_cursor_usn < server_sync_cursor_usn，服务器在 [client_sync_cursor_usn, server_sync_cursor_usn) 有客户端未拉取的数据。

#### OVERWRITE_REQUIRED

以下场景会触发：
client_sync_cursor_usn > server_sync_cursor_usn，这种情况下可能是由于服务器回滚、恢复旧备份或云端数据被重置。
collection_schema_updated_at 不一致，例如模板、字段、牌组结构冲突，无法安全增量合并。
该状态下用户需要选择覆盖方向：
1. 本地覆盖服务器
2. 服务器覆盖本地


#### LOCKED_BY_OTHER_CLIENT

其他设备正在同步


#### CLIENT_TOO_OLD

客户端 protocol_version 或 db_schema_version 低于服务端最低支持版本，需要升级客户端。


#### SERVER_TOO_OLD
客户端 protocol_version 高于服务端当前支持版本，需要升级服务端。


#### ConnectRPC 全局错误

以下情况不进入 HandshakeStatus，而是由 Go 服务端返回 ConnectRPC error，客户端在 catch 分支读取 error code：

| 场景 | ConnectRPC code | 说明 |
| --- | --- | --- |
| access token 缺失、过期、无效 | `Unauthenticated` | 全局认证失败，通常在 interceptor 中处理，业务 handler 可以不进入 |
| 用户无权访问 collection | `PermissionDenied` | 资源授权失败，不属于握手业务状态 |
| Protobuf 字段或 buf.validate 校验失败 | `InvalidArgument` | 请求格式非法，例如 UUID 长度不对或 `client_sync_cursor_usn < 0` |
| 全局限流、配额耗尽 | `ResourceExhausted` | 等价于 too many requests，不属于握手业务状态 |
| 服务维护、依赖暂时不可用 | `Unavailable` | 可以提示稍后重试 |
| 客户端 deadline 超时 | `DeadlineExceeded` 或客户端本地超时 | 客户端没有拿到可信 HandshakeResponse |
| 服务端非预期异常 | `Internal` | 服务端未能完成握手业务判定 |

`UNAUTHORIZED`、`TOO_MANY_REQUESTS` 和请求中的 `PENDING` 不属于 HandshakeStatus。前两者是全局 RPC 错误，`PENDING` 是客户端本地 UI/请求中状态。


#### 握手过程，服务器或网络意外的处理

首先客户端定时器设置超时时间为 10 秒

为了避免服务器或网络意外，可能用户手动点重复同步导致重复发握手消息，所以服务端创建一个 TTL 为 60 秒的用户级分布式锁 SyncLock。

如果客户端未收到 Handshake 响应，视为本次 RPC 结果未知。未知导致的情况由 SyncLock 处理：若再次握手时 SyncLock 仍存在，则根据device_id 判断是否为同一设备恢复当前 session（TODO 根据复杂度决定是否实现）；否则返回 LOCKED_BY_OTHER_CLIENT。若 SyncLock 已过期，则重新创建 session。

## 数据同步

### 状态机图

（这里是一个客户端状态机图，在飞书上）

### 注意事项

UUID 选择 string 类型，要求是使用连字符分隔的 36 个字符的格式。之所以不用 bytes 类型是因为可能存在大端序/小端序问题，避免不同语言不同库下表现不一致。

### 数据 Payload 设计

enlangmemo/sync/v1/entities.proto 定义同步数据的 Payload 是咋样的，除了不带 usn 基本上与 https://dbdiagram.io/d/EnLangMemo-69aafcb1a3f0aa31e1146507 表结构一致。

collection 的 payload 还少了下面这两个字段
```DBDiagram
// 全局同步状态
last_sync_time integer [not null, default: 0]
sync_cursor_usn integer [not null, default: 0]
```

`last_sync_time` 是客户端本地辅助字段，表示该客户端上一次完整同步成功完成的服务端时间。它不参与同步一致性判断，不进入 CollectionPayload；客户端只在 FinishSync 成功返回后写入 `FinishSyncResponse.server_finished_at`。`sync_cursor_usn` 是客户端已同步到的 usn 上界 / 下次增量 Pull 的起点，会在 Pull / Push batch 本地事务成功后推进。


### PULLING 状态

PULLING 表示客户端正在从服务端拉取当前同步会话范围内的远端增量数据。

进入 PULLING 的前提是 HandshakeResponse 返回 `NEED_PULL`，客户端持有本次同步会话的 `session_id`，并且服务端 SyncLock 中保存了本轮 Pull 的起点 `client_sync_cursor_usn_at_handshake` 与远端上界 `server_sync_cursor_usn_at_handshake`。

Pull 的同步颗粒度是 batch。一个 batch 对应客户端本地的一次 SQLite 事务，batch 不一定只与单个 usn 绑定。客户端不指定 `batch_size`，服务端按 usn 升序返回变更。
每条 `SyncChange` 自带该实体变更对应的服务端 usn。

注意：不会出现同步的变更 usn = a 时，后面批的变更又出现相同的 usn = a，一个 usn 值的数据会一次性拉完，而不会出现这个 usn = a 拉了一半，另一半放在下一个 batch 的情况。

#### PullRequest

```proto
message PullRequest {
  string session_id = 1 [(buf.validate.field).string.len = 32];

  // 当前请求的 batch 序号，从 1 开始
  // 服务端校验 batch_seq == SyncLock.expected_batch_seq
  int32 batch_seq = 2 [(buf.validate.field).int32.gte = 1];
}
```

`batch_seq` 用于保证请求顺序。服务端必须根据 SyncLock 中的 session 状态决定当前 batch 从哪里继续读取。

#### PullResponse

```proto
message PullResponse {
  // 当前返回的 batch 序号，等于 request.batch_seq
  int32 batch_seq = 1;

  // 本 batch 最大的 usn
  int64 batch_max_usn = 2 [(buf.validate.field).int64.gte = 0];

  // 这批数据的全部变更
  repeated SyncChange changes = 3 [(buf.validate.field).repeated.min_items = 1];

  // 是否为本轮 Pull 的最后一个 batch
  bool last_batch = 4;
}
```

`changes` 中每条 `SyncChange` 表示一个实体变更。这里的实体指的是卡片模板、卡片、复习记录等等，详细节见 SyncChange 的定义。PullResponse 中每条 `SyncChange.usn` 必须是服务端已确认的实体 usn，且应按 usn 升序返回。

`batch_max_usn` 表示本 batch 内所有 `changes.usn` 的最大值，用于让客户端粗略地校验，避免某次迭代服务端有问题。正常情况下 `batch_max_usn == max(changes.usn)`。

`ChangeOp = UPSERT` 时必须携带 `payload`

`ChangeOp = DELETE` 时不携带 payload，客户端根据 `entity_id` 和 `entity_type` 删除对应本地数据库数据

#### 客户端处理流程

1. 客户端进入 PULLING 后，从 `batch_seq = 1` 开始发送 PullRequest
2. 客户端收到 PullResponse 后，校验 `response.batch_seq == request.batch_seq`
3. 客户端开启一个本地 SQLite 事务，按服务端返回顺序应用本 batch 的全部 `changes`
4. 客户端计算本 batch 内最大 `change.usn`，并校验其等于 `response.batch_max_usn`；不一致时视为协议错误，本次同步失败
5. 同一个事务内，`UPSERT` 实体的 `usn` 写为对应 `change.usn`；`DELETE` 按删除语义处理；全部变更应用成功后，客户端必须在每个 batch 成功应用后将 `collection.sync_cursor_usn` 推进到 `response.batch_max_usn + 1`
6. 事务提交后，如果 `last_batch = false`，客户端发送下一个 `batch_seq + 1` 的 PullRequest。
7. 如果 `last_batch = true`，表示本轮 Pull 范围内的远端增量已全部落库。
客户端此时的 `collection.sync_cursor_usn` 应等于本次会话的 `server_sync_cursor_usn`。
8. Pull 完成后，如果本地仍存在 `usn = -1` 的未同步数据，客户端进入 PUSHING；否则进入 FINISHING，调用 FinishSync 释放 session / SyncLock。

#### 服务端处理规则

1. 服务端收到 PullRequest 后，先校验 `session_id` 是否存在、是否属于当前用户、SyncLock 是否仍在 PULLING 状态。
2. 服务端校验 `request.batch_seq == SyncLock.expected_batch_seq`；不一致时返回 ConnectRPC `FailedPrecondition`。
3. 服务端在当前同步会话保存的 `[client_sync_cursor_usn_at_handshake, server_sync_cursor_usn_at_handshake)` 范围内，按 usn 升序选择下一批待发送变更。
4. 服务端为每条 `SyncChange` 写入该实体变更对应的 usn，计算并写入 `batch_max_usn = max(changes.usn)`，并设置 `last_batch` 表示该 batch 是否已经覆盖本轮 Pull 的上界。
5. 服务端先将 `SyncLock.expected_batch_seq` 递增 1，确认更新成功后，再返回 PullResponse。
6. 如果当前 batch 是本轮 Pull 的最后一个 batch，服务端将 SyncLock 状态从 PULLING 改为 AWAITING_CLIENT_ACTION 状态（服务器特有的）。

#### 中断与超时

Pull 不额外设计 ACK。客户端只有在本地事务提交成功后，才继续请求下一个 batch。

如果 PullRequest 超时、网络中断、客户端崩溃或本地事务失败，客户端不继续猜测当前 batch 状态，直接视为本次同步结束。

### PUSHING

PUSHING 表示客户端正在把本地 `usn = -1` 的未同步变更上传到服务端。

进入 PUSHING 有两种情况：

1. HandshakeResponse 返回 `NO_REMOTE_CHANGES` 后，服务端进入 AWAITING_CLIENT_ACTION；如果客户端本地存在 `usn = -1` 的未同步数据，则客户端发送第一个 PushRequest 进入 PUSHING。
2. PULLING 完成后，客户端本地仍存在 `usn = -1` 的未同步数据；客户端向服务端发送第一个 PushRequest，服务端从 AWAITING_CLIENT_ACTION 切换到 PUSHING。

Push 的同步颗粒度也是 batch。一个 Push batch 对应服务端一次数据库事务，服务端为该 batch 分配一个新的 usn，并将 batch 内所有变更写为同一个 usn。客户端上传时本地未同步变更的 `SyncChange.usn` 为 -1，该值表示待上传

#### PushRequest

```proto
message PushRequest {
  string session_id = 1 [(buf.validate.field).string.len = 32];

  // 当前请求的 batch 序号，从 1 开始
  // 服务端校验 batch_seq == SyncLock.expected_batch_seq
  int32 batch_seq = 2 [(buf.validate.field).int32.gte = 1];

  // 客户端本地未同步的一批变更，每条 SyncChange.usn 必须为 -1
  repeated SyncChange changes = 3 [(buf.validate.field).repeated.min_items = 1];

  // 是否为本轮 Push 的最后一个 batch
  bool last_batch = 4;
}
```

PushRequest 中每条 `SyncChange.usn` 必须为 `-1`。`usn = -1` 表示客户端本地待上传状态，服务端要自己分配一个新的 usn 并写入数据库，用当前的 server_sync_cursor_usn 作为本 batch 的 usn，然后 usn 自增。

#### PushResponse

```proto
message PushResponse {
  // 当前返回的 batch 序号，等于 request.batch_seq
  int32 batch_seq = 1;

  // 服务端为本 Push batch 分配的 usn
  // 客户端用它更新本 batch 内已上传实体的 usn，并推进 collection.sync_cursor_usn = assigned_usn + 1。
  int64 assigned_usn = 2 [(buf.validate.field).int64.gte = 0];
}
```

`assigned_usn` 是服务端为本 Push batch 分配的 usn。客户端收到 PushResponse 后，用它更新本 batch 内所有已上传实体的 `usn`，并将 `collection.sync_cursor_usn` 推进到 `assigned_usn + 1`。如果本 batch 包含 Collection 变更，collection 表自身的 `usn` 也写为 `assigned_usn`。

#### 客户端处理流程

1. 客户端进入 PUSHING 后，从 `batch_seq = 1` 开始发送 PushRequest。
2. 客户端从本地 `usn = -1` 的待上传数据中组装当前 batch，按实体依赖顺序放入 `changes`。
3. 发送 PushRequest 后等待 PushResponse
4. 收到 PushResponse 后，校验 `response.batch_seq == request.batch_seq`。
5. 客户端开启一个本地 SQLite 事务，将本 batch 内已上传实体的 `usn` 更新为 `response.assigned_usn`，并将 `collection.sync_cursor_usn` 推进到 `response.assigned_usn + 1`。
6. 事务提交后，如果本次 `request.last_batch = false`，客户端发送下一个 `batch_seq + 1` 的 PushRequest。
7. 如果本次 `request.last_batch = true`，客户端进入 FINISHING，调用 FinishSync 释放 session / SyncLock。

#### 服务端处理规则

1. 服务端收到 PushRequest 后，先校验 `session_id` 是否存在、是否属于当前用户、SyncLock 是否处于允许 Push 的状态。
2. 如果当前状态是 AWAITING_CLIENT_ACTION，并且 `request.batch_seq = 1`，服务端将 SyncLock 状态切换为 PUSHING。
3. 如果当前状态是 PUSHING，服务端校验 `request.batch_seq == SyncLock.expected_batch_seq`，不匹配时返回 ConnectRPC `FailedPrecondition`。
4. 服务端校验：
   - `changes` 非空
   - PushRequest 中每条 `SyncChange.usn` 必须为 `-1`
   - 当 `ChangeOp = UPSERT` 时，确认 payload 与 `entity_type` 匹配
   - 当 `ChangeOp = DELETE` 时，payload 为空。
   - 校验失败返回 ConnectRPC `InvalidArgument`
5. 服务端开启数据库事务，为当前 batch 分配一个新的 usn，将 batch 内所有变更写入数据库；`UPSERT` 实体的 `usn` 写为该 usn，`DELETE` 按删除语义处理。
6. 服务端先将 `SyncLock.expected_batch_seq` 递增 1；如果 `request.last_batch = true`，同时将 SyncLock 状态改为 AWAITING_FINISH。
7. 服务端确认数据库事务与 SyncLock 更新都成功后，返回 PushResponse，其中 `assigned_usn` 为本 batch 分配的 usn。

#### 中断与超时

Push 同样不额外设计 ACK。客户端只有在收到 PushResponse，并成功更新本地 `usn` 和 `collection.sync_cursor_usn` 后，才继续发送下一个 batch。

`last_sync_time` 不在 Push batch 内更新。它表示一次完整同步成功完成的服务端时间，应该在 FinishSync 成功返回后由客户端使用 `server_finished_at` 统一更新。

如果 PushRequest 超时、网络中断、客户端崩溃或本地事务失败，客户端直接视为本次同步结束。

同步数据塞进数据库后，即使客户端没收到响应，也不会导致数据不一致。因为后续重新发起同步时，由于没收到响应本地的 usn 会落后服务端，会先 Pull 再 Push，保证数据一致。

### FINISHING 状态

FINISHING 表示同步数据传输已经完成，客户端正在通知服务端结束当前同步会话，释放 SyncLock。

客户端进入 FINISHING 有三种情况：

1. HandshakeResponse 返回 `NO_REMOTE_CHANGES`，并且客户端本地没有 `usn = -1` 的未同步数据。
2. PULLING 完成后，客户端本地没有 `usn = -1` 的未同步数据。
3. PUSHING 完成后，客户端已经成功处理最后一个 PushResponse。

#### FinishSyncRequest

```proto
message FinishSyncRequest {
  string session_id = 1 [(buf.validate.field).string.len = 32];
}
```

FinishSyncRequest 必须携带 `session_id`，服务端用它确认客户端结束的是当前同步会话，避免误释放其他 session。

#### FinishSyncResponse

```proto
message FinishSyncResponse {
  // 服务端确认完成同步并释放 session / SyncLock 的时间
  //
  // 客户端用其更新本地 collection.last_sync_time
  int64 server_finished_at = 1 [(buf.validate.field).int64.gte = 0];
}
```

FinishSyncResponse 成功返回即表示 FinishSync ACK：服务端已经接受本次完成请求，并释放当前 session / SyncLock。`server_finished_at` 是服务端确认完成同步的时间，客户端用它更新本地 `collection.last_sync_time`。

#### 客户端处理流程

1. 客户端进入 FINISHING 后，发送 FinishSyncRequest。
2. 客户端收到 FinishSyncResponse 成功响应后，进入 FINISHED。
3. 客户端在进入 FINISHED 时，将本地 `collection.last_sync_time` 更新为 `response.server_finished_at`。

#### 服务端处理规则

1. 服务端收到 FinishSyncRequest 后，先校验 `session_id` 是否存在、是否属于当前用户、SyncLock 是否处于允许 Finish 的状态。
2. 允许 Finish 的状态包括 AWAITING_CLIENT_ACTION 和 AWAITING_FINISH。
3. 校验通过后，服务端释放当前 session / SyncLock。
4. 服务端确认释放成功后，返回 FinishSyncResponse，并将当前服务端时间写入 `server_finished_at`。

如果 `session_id` 不存在、已经过期、属于其他用户或状态不允许 Finish，服务端返回 ConnectRPC `FailedPrecondition`。

#### 中断与超时

如果 FinishSyncRequest 超时、网络中断或客户端崩溃，客户端不能确认服务端是否已经释放 SyncLock。客户端直接视为本次同步结束，不更新 `last_sync_time`。

如果服务端已经释放 SyncLock，但客户端没有收到 FinishSyncResponse，不过此时数据都已经同步完成，唯独 `last_sync_time` 没有更新，而这个字段仅用于客户端展示上次同步时间，不影响数据一致性。





