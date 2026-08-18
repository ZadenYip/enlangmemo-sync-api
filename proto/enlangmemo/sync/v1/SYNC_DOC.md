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


# 流程

## 流程简述

1. 双方握手
2. 根据握手状态客户端决定下一步：
  - NO_REMOTE_CHANGES：远端无新增。客户端若有本地待上传变更，则 PUSH，否则结束。
  - NEED_PULL：远端有新增。客户端先 Pull，Pull 完成后客户端根据本地剩余待上传变更决定 PUSH 或结束。
  - UPLOAD_ALL：客户端同步游标大于服务端同步游标，属于极端情况下服务端数据丢失/回退；客户端全量上传本地数据恢复服务端，完成后进入 FINISHING。
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

  // 客户端发起握手时的本地时间戳
  int64 client_now = 7 [(buf.validate.field).int64.gte = 0];

  // 客户端 collection.last_sync_time，上一次完整同步成功完成的服务端时间
  int64 client_last_sync_time = 8 [(buf.validate.field).int64.gte = 0];

  // 客户端当前是否存在待上传的本地变更，包括 usn = -1 的 UPSERT 和 tombstone 删除记录
  bool has_local_changes = 9;
}
```

`device_id` 用于区分同一用户的不同设备，并辅助判断 SyncLock 存在时是否为同一设备重复握手。

`client_now` 表示客户端发起握手时的本地时间戳。服务端用其对比自身时间，若偏差过大则返回 `CLIENT_TIME_SKEW_TOO_LARGE`，提示用户校准系统时间再同步。

`client_last_sync_time` 表示客户端本地记录的上一次完整同步成功时间，该值来自上次 `FinishSyncResponse.server_finished_at`，因此是服务端分配的可信时间。服务端用它判断客户端是否落后过久。如果服务端运维正式删除某个时间之前的实体（原本是用 delete 标记逻辑删除的），而客户端最后同步时间如果比这个时间点早，则握手会返回 `CLIENT_DATA_TOO_OLD`。

`has_local_changes` 表示客户端握手时是否存在待上传的本地变更，包括本地实体 `usn = -1` 的 UPSERT，以及 tombstones 中待上传的 DELETE。服务端用其决定 `NO_REMOTE_CHANGES` 下握手后的初始状态。

### Collection 身份校验

每个用户在服务端只有一个 collection。HandshakeRequest 中的 `collection_id` 表示客户端本地唯一 collection 身份，服务端必须在判断 `client_sync_cursor_usn` 前先校验该身份。

注意：即使集合被标记为已删除（比如 UPLOAD_ALL 干的），即使这样，服务端仍然能校验客户端的 `collection_id` 是否属于当前账号。

如果用户还没有 collection 记录，则服务端可以将本次请求的 `collection_id` 绑定为该用户唯一 collection。如果用户已经有绑定记录，则 `request.collection_id` 必须等于服务端记录的 `collection_id`。

如果 `collection_id` 不一致，本次握手不进入 HandshakeStatus，服务端直接返回 ConnectRPC `FailedPrecondition`，表示请求格式和用户认证都有效，但当前本地 collection 身份与该账号已绑定的云端 collection 不匹配，不能继续同步，也没法触发 `UPLOAD_ALL`。

### SyncLock
SyncLock 为 Redis 用户级服务端分布式锁，也起到维护 session 的作用。

在验证 access_token 有效后并解析出 user_id 后才会存这个分布式锁，而在 TTL 到了或者用户完成了后续同步操作后才会释放。

目的是禁止同步期间用户用其他设备同时进行同步，避免造成用户自己设备并发同步造成一致性问题，可以理解为类似于互斥锁的作用。

若再次握手时 SyncLock 仍存在，则根据 device_id 判断是否为同一设备恢复当前 session。
此时的 TTL 为 1 分钟, key 为用户 sync:{userID}:sync_lock

```json
{
    "user_id": xxx, // 利用 userID 锁住用户其他设备的同步操作
    "state": xxx, // 处于同步的什么状态（阶段）
    // 是当前 state 内期望的下一个 batch 序号；进入 PULLING、PUSHING、UPLOADING_ALL 或 AWAITING_PUSH_OR_FINISH 时重置为 1。
    "expected_batch_seq": xxx,
    // 当前 session 内部使用的 USN 游标。PULLING 中表示当前实体类型内的拉取游标
    "sync_cursor_usn": xxx,
    // PULLING 中尚未拉完的实体类型队列，字符串形式保存，例如 "1,2,4,6"
    "pull_entity_queue": xxx,
    "session_id": xxx,
    "client_sync_cursor_usn_at_handshake": xxx,
    "server_sync_cursor_usn_at_handshake": xxx,
    "device_id": xxx
}
```

`sync_cursor_usn` 是当前 session 内部使用的同步游标：PULLING 中表示当前实体类型内的拉取游标，PUSHING 中表示服务端下一次要分配的 usn，UPLOADING_ALL 中表示已恢复客户端快照的 usn 上界。

`pull_entity_queue` 只在 PULLING 中使用，表示当前还没拉完的实体类型队列。第一个元素是当前正在拉取的实体类型；字段存储为字符串，由服务端应用层解析，不要求 Redis 存数组。


### HandshakeResponse
HandshakeResponse 表示此次握手结果。
握手结果有以下：

```proto
message HandshakeResponse {
  HandshakeStatus status = 1;

  // 服务端随机生成的 16 字节 session_id（转为字符串后长度为 32）
  // 只有需要继续同步会话时才返回，NO_REMOTE_CHANGES 且 has_local_changes = false 时不返回
  optional string session_id = 2 [(buf.validate.field).string.len = 32];

  int64 server_sync_cursor_usn = 3 [(buf.validate.field).int64.gte = 0];

  // 服务端 collection.last_sync_time，客户端收到握手响应后根据情况用它覆盖本地 collection.last_sync_time
  int64 server_last_sync_time = 4 [(buf.validate.field).int64.gte = 0];
}

enum HandshakeStatus {
  HANDSHAKE_STATUS_UNSPECIFIED = 0;

  // client_sync_cursor_usn == server_sync_cursor_usn
  HANDSHAKE_STATUS_NO_REMOTE_CHANGES = 1;
  // client_sync_cursor_usn < server_sync_cursor_usn
  HANDSHAKE_STATUS_NEED_PULL = 2;

  // client_sync_cursor_usn > server_sync_cursor_usn，客户端需要全量上传本地数据恢复服务端
  HANDSHAKE_STATUS_UPLOAD_ALL = 3;

  // 其他客户端正在进行同步
  HANDSHAKE_STATUS_LOCKED_BY_OTHER_CLIENT = 4;

  // 客户端 protocol_version 或 db_schema_version 低于服务端最低支持版本
  HANDSHAKE_STATUS_CLIENT_TOO_OLD = 5;
  // 客户端 protocol_version 高于服务端当前支持版本
  HANDSHAKE_STATUS_SERVER_TOO_OLD = 6;

  // 客户端本地时间与服务端时间偏差过大
  HANDSHAKE_STATUS_TIME_SKEW_TOO_LARGE = 7;

  // 客户端数据落后过久，服务端已正式删除被 delete 标记的数据，客户端需要重置本地数据后重新同步
  HANDSHAKE_STATUS_CLIENT_DATA_TOO_OLD = 8;
}

```

session_id 由服务端在允许继续当前同步会话时生成，用于标识本次同步会话。

`NEED_PULL`、`UPLOAD_ALL` 以及 `NO_REMOTE_CHANGES + has_local_changes = true` 下会返回 session_id。`NO_REMOTE_CHANGES + has_local_changes = false` 表示本次只是空同步检查，服务端不创建 SyncLock。

字段存在时必须是 32 位字符串，后续 Pull / Push / FinishSync 都需要携带 session_id。

`server_last_sync_time` 表示服务端记录的上一次完整同步成功完成时间。客户端收到可信 HandshakeResponse 后，对比本地的 USN 和服务器的 USN，如果相同则用该值覆盖本地 `collection.last_sync_time`，用于修正 FinishSyncResponse 丢失但实际已经完成同步的场景。


#### NO_REMOTE_CHANGES
NO_REMOTE_CHANGES 表示 client_sync_cursor_usn == server_sync_cursor_usn，服务器相对客户端同步游标无新增数据。服务端根据 `has_local_changes` 设置握手后的初始状态：
如果为 true，则进入 PUSHING，`expected_batch_seq = 1`，`sync_cursor_usn = server_sync_cursor_usn_at_handshake`。
如果为 false，则不创建 SyncLock、不返回 `session_id`，客户端直接结束本次同步检查。


#### NEED_PULL

client_sync_cursor_usn < server_sync_cursor_usn，服务器在 [client_sync_cursor_usn, server_sync_cursor_usn) 有客户端未拉取的数据。

NEED_PULL 下服务端进入 PULLING，`expected_batch_seq = 1`，`sync_cursor_usn = client_sync_cursor_usn_at_handshake`，并初始化 `pull_entity_queue`。Pull 完成后服务端统一进入 AWAITING_PUSH_OR_FINISH，`expected_batch_seq = 1`，由客户端决定继续 Push 还是直接 FinishSync。


#### UPLOAD_ALL

以下场景会触发：
client_sync_cursor_usn > server_sync_cursor_usn，这种情况下可能是由于服务器回滚、恢复旧备份或云端数据被重置。
该状态表示客户端已经确认过的同步游标（cursor_usn）超过服务端的，普通的 Pull 没法处理。客户端应进入 UPLOAD_ALL，将本地 collection 当前数据全量上传到服务端，用于恢复服务端缺失的数据。

Handshake 返回 `UPLOAD_ALL` 时，服务端创建 SyncLock，返回 `session_id`，并将 SyncLock 状态设置为 AWAITING_UPLOAD_ALL_CONFIRM，`expected_batch_seq = 1`。此时服务端还没有删除或覆盖正式数据，只是在等待客户端用户确认。

客户端收到 `UPLOAD_ALL` 后，需要提示用户确认：服务器数据可能丢失，需要使用本机数据恢复服务器。

- 用户确认：客户端先发送 UploadAllPrepareRequest。
- 用户取消：客户端发送 CancelSyncRequest，服务端校验 `session_id` 后释放 SyncLock，并在 CancelSyncResponse 中回传同一个 `session_id`，客户端需要校验响应中的 `session_id` 与请求一致。

服务端在 AWAITING_UPLOAD_ALL_CONFIRM 状态只接受 UploadAllPrepareRequest 或 CancelSyncRequest。

```proto
message UploadAllPrepareRequest {
  string session_id = 1 [(buf.validate.field).string.len = 32];
}

message UploadAllPrepareResponse {
  string session_id = 1 [(buf.validate.field).string.len = 32];
}

message UploadAllPushRequest {
  string session_id = 1 [(buf.validate.field).string.len = 32];

  // 当前请求的 batch 序号，从 1 开始
  // 服务端校验 batch_seq == SyncLock.expected_batch_seq
  int32 batch_seq = 2 [(buf.validate.field).int32.gte = 1];

  // 客户端完整快照中的一批已确认变更，每条 SyncChange.usn 必须 > 0
  repeated SyncChange changes = 3 [(buf.validate.field).repeated.min_items = 1];

  // 是否为本轮 UploadAllPush 的最后一个 batch
  bool last_batch = 4;
}

message UploadAllPushResponse {
  string session_id = 1 [(buf.validate.field).string.len = 32];

  // 当前返回的 batch 序号，等于 request.batch_seq
  int32 batch_seq = 2;
}

message CancelSyncRequest {
  string session_id = 1 [(buf.validate.field).string.len = 32];
}

message CancelSyncResponse {
  string session_id = 1 [(buf.validate.field).string.len = 32];
}
```

UploadAllPrepareRequest 成功后，服务端在一个独立事务中执行恢复准备：标记该用户当前 collection 下的旧服务端同步数据为已删除，更新对应 `sync_units` 删除标记，并将服务端 collection 的同步游标重置为 0，使后续中断后再次握手仍会进入 `UPLOAD_ALL`。Prepare 成功后，服务端将 SyncLock 状态切换为 UPLOADING_ALL，`expected_batch_seq = 1`，`sync_cursor_usn = 0`，并在 UploadAllPrepareResponse 中回传同一个 `session_id`，客户端需要校验响应中的 `session_id` 与请求一致。

进入 UPLOADING_ALL 后，客户端先分 batch 上传所有 `usn > 0` 的已确认本地数据。UPLOAD_ALL 的数据 batch 使用 UploadAllPushRequest / UploadAllPushResponse，而不是普通的 PushRequest。

UPLOAD_ALL 时客户端已经确认过的本地数据以客户端当前 `SyncChange.usn` 为准，服务端必须按该 usn 恢复数据和 `sync_units`。UploadAllPushRequest 中每条 `SyncChange.usn` 必须 `> 0`；如果存在 `usn = -1` 的本地未确认变更，不能放入 UploadAllPushRequest，必须等 UploadAllPush 完成后再按普通 Push 逻辑上传。

UPLOADING_ALL 状态下，服务端只接受 UploadAllPushRequest，并校验 `request.batch_seq == SyncLock.expected_batch_seq`。每个 UploadAllPush batch 对应一个服务端数据库事务；batch 成功后服务端计算 `batch_max_usn = max(changes.usn)`，并更新 `sync_cursor_usn = max(sync_cursor_usn, batch_max_usn + 1)`。

如果 `last_batch = false`，服务端递增 `expected_batch_seq`，继续保持 UPLOADING_ALL。如果 `last_batch = true`，服务端校验 `sync_cursor_usn == client_sync_cursor_usn_at_handshake`；校验通过后，将服务端 collection.sync_cursor_usn 写为 `sync_cursor_usn`，状态进入 AWAITING_FINISH。

UploadAllPush 完成后，客户端发送 FinishSyncRequest 结束本次 UPLOAD_ALL 会话。如果客户端还有 `usn = -1` 的本地未确认变更，由客户端在 FinishSync 成功后重新发起一轮普通同步，并通过普通 Push 上传。

如果 UploadAllPrepare 已成功但后续上传中断，后续同步仍会因为服务端同步游标为 0 而重新进入 `UPLOAD_ALL`，直到全量上传完成并 FinishSync 成功释放 SyncLock。

这里采取全量同步的考虑原因见下：

因为服务器的数据丢失，客户端版本超前。因此，可能出现客户端已经删除的数据 A 在服务器上依然存在，可客户端很可能 tombstone 没有对记录 A 删除的标记了，所以没法通过 tombstone 标记删除服务器已有的改记录 A。

所以，为了数据一致性这时候采取全量覆盖服务器数据。


#### LOCKED_BY_OTHER_CLIENT

其他设备正在同步


#### CLIENT_TOO_OLD

客户端 protocol_version 或 db_schema_version 低于服务端最低支持版本，需要升级客户端。


#### SERVER_TOO_OLD
客户端 protocol_version 高于服务端当前支持版本，需要升级服务端。


#### CLIENT_TIME_SKEW_TOO_LARGE

客户端 `client_now` 与服务端当前时间偏差超过服务端允许阈值。该状态不创建可继续同步的 session，不返回 `session_id`。客户端应提示用户校准系统时间后重新同步。

#### CLIENT_DATA_TOO_OLD

客户端 `client_last_sync_time` 比服务端运维正式删除了标记数据的时间还要早，因为服务端删除了客户端要利用同步删除本地数据的 delete 记录。

此时普通 Pull 可能无法让客户端知道某些旧实体已经在服务端被删除，因此服务端拒绝本次增量同步，不返回 `session_id`。

客户端收到该状态后，提示用户当前本地数据已经落后太久，需要先清空本地 collection 数据。用户点击重置后，客户端清理本地同步数据，清理完后再重新发起同步。重置完成前不允许正常同步。


#### ConnectRPC 全局错误

以下情况不进入 HandshakeStatus，而是由 Go 服务端返回 ConnectRPC error，客户端在 catch 分支读取 error code：

| 场景 | ConnectRPC code | 说明 |
| --- | --- | --- |
| access token 缺失、过期、无效 | `Unauthenticated` | 全局认证失败，通常在 interceptor 中处理，业务 handler 可以不进入 |
| 用户无权访问 collection | `PermissionDenied` | 资源授权失败，不属于握手业务状态 |
| Protobuf 字段或 buf.validate 校验失败 | `InvalidArgument` | 请求格式非法，例如 UUID 长度不对或 `client_sync_cursor_usn < 0` |
| 用户已有服务端 collection，但请求中的 collection_id 与服务端记录不一致 | `FailedPrecondition` | 本地 collection 身份与账号已绑定的云端 collection 不匹配，不能继续同步 |
| 全局限流、配额耗尽 | `ResourceExhausted` | 等价于 too many requests，不属于握手业务状态 |
| 服务维护、依赖暂时不可用 | `Unavailable` | 可以提示稍后重试 |
| 客户端 deadline 超时 | `DeadlineExceeded` 或客户端本地超时 | 客户端没有拿到可信 HandshakeResponse |
| 服务端非预期异常 | `Internal` | 服务端未能完成握手业务判定 |

`UNAUTHORIZED`、`TOO_MANY_REQUESTS` 和请求中的 `PENDING` 不属于 HandshakeStatus。前两者是全局 RPC 错误，`PENDING` 是客户端本地 UI/请求中状态。


#### 握手过程，服务器或网络意外的处理

首先客户端定时器设置超时时间为 10 秒

为了避免服务器或网络意外，可能用户手动点重复同步导致重复发握手消息，所以服务端创建一个 TTL 为 60 秒的用户级分布式锁 SyncLock。

如果客户端未收到 Handshake 响应，视为本次 RPC 结果未知。未知导致的情况由 SyncLock 处理：若再次握手时 SyncLock 仍存在，则根据 device_id 判断是否为同一设备恢复当前 session（TODO 根据复杂度决定是否实现）；否则返回 LOCKED_BY_OTHER_CLIENT。若 SyncLock 已过期，则重新创建 session。

## 数据同步

### 状态机图

（这里是一个客户端状态机图，在飞书上）


### 注意事项

UUID 选择 string 类型，要求是使用连字符分隔的 36 个字符的格式。之所以不用 bytes 类型是因为可能存在大端序/小端序问题，避免不同语言不同库下表现不一致。


### 数据 Payload 设计

v1/entities.proto 定义同步数据的 payload。payload 基本与当前 SQLite 表结构一致，但不包含实体 id，而实体 id 统一使用外层 `SyncChange.entity_id`。

`dic_note_map` 属于客户端本地配置，这个属于本地客户端配置摘录词的映射到哪个模板用的功能，因为还没确定正式版所以不参与同步。

notes 表中的 `sort_field` 和 `search_fields` 属于客户端根据 `fields` 清洗后计算出的本地派生字段，不传给服务端，也不参与同步。

collection 的 payload 不包含客户端 SQLite 表下面这个字段：

```DBDiagram
// 全局同步状态
last_sync_time integer [not null, default: 0]
sync_cursor_usn integer [not null, default: 0]
```

`last_sync_time` 是客户端本地辅助字段，展示给用户上次同步时间。该值以服务端记录为准，只在 FinishSync 和 HandshakeResponse 时更新。

客户端只写入服务端返回的时间值，不能用本地当前时间更新 `last_sync_time`。

`sync_cursor_usn` 是客户端已同步到的 usn 上界 / 下次增量 Pull 的起点，会在 Pull / Push batch 本地事务成功后推进。

note_types 的字段结构约束由客户端业务层保证：不支持在同一个 `note_type.id` 上原地新增、删除或重排字段；如果需要改变字段集合，应创建新的 note_type。修改 `css`、`front`、`back`、`sortField` 等展示模板内容只需要同步 `note_types` 自身，不要求引用它的 notes/cards 一起变为 `usn = -1`。

服务端不校验 note_type 字段集合、notes.fields 与 note_type.fields 是否匹配、deck 配置语义等业务规则，服务端主要负责处理 session、batch_seq、usn 分配、UPSERT / DELETE 持久化等同步协议规则。

### 同步实体顺序

Pull 按实体类型顺序拉取，Push 传输 UPSERT 变更时也“优先”按下面顺序组织：

1. Collection
2. Deck
3. NoteType
4. Note
5. ProcessingNote
6. Card
7. ReviewLog

DELETE 变更也按上面的优先级组织。不过具体的 DELETE 的应用逻辑与 UPSERT 有些不同，详细见下面。


### 服务端同步索引表

服务端维护一张 `sync_units` 表用于记录每个同步实体当前最新的服务端同步状态。`sync_units`，注意这个不是仅追加的表，同一实体再次 UPSERT / DELETE 时，会更新已有的记录。

字段：

```text
sync_units
- user_id
- entity_id
- entity_type
- usn
- op
- updated_at
```

主键与索引：

```text
PRIMARY KEY (user_id, entity_id)
INDEX ix_sync_units_pull_entity (user_id ASC, entity_type ASC, usn ASC)
INDEX ix_sync_units_delete_retention (op, updated_at)
```

服务端在是在同一个数据库事务内同时写入业务表、`sync_units`。

`updated_at` 用于服务端运维清理过期 DELETE marker。

服务端可以删除 `op = CHANGE_OP_DELETE` 且 `updated_at` 早于删除标记保留边界的 `sync_units` 行

握手时如果客户端的 `client_last_sync_time` 比这个时间边界早，则返回 `CLIENT_DATA_TOO_OLD`，要求客户端重置本地数据后再同步。


### 删除同步策略

同步删除采用删除优先策略，也就是只要任何一端有删除意图，所有终端的数据都进行同步后这个数据不会再出现在设备中。

当客户端本地删除数据后，因为数据记录删除后，数据本身就已经不存在了，所以也无法告知服务器，因此引入 tombstones 表记录本地已经删除的数据，同步上传给服务器确认删除后，同步的时候会上传 tombstones 的记录，服务器确认后，该表下的记录即可删除，这样子就完成了告知服务器客户端删除数据的操作。

因此 tombstones 需要  `entity_type`、`entity_id`、`deleted_at`，用来表示删除的是哪个表哪个实体的数据，以及时间。

#### 客户端删除策略
PULLING 远端变更前，客户端必须先检查同一 `entity_type` 和 `entity_id` 是否存在本地 tombstone。
- 如果本地有对应的 tombstone 记录，说明本地已经存在删除意图，客户端可以直接确认该删除并清理 tombstone，不需要再查询正式表删除数据。
- 如果本地没有对应的 tombstone 记录，客户端需要另外从正式表查找并删除对应实体
- 收到远端 UPSERT 时，而数据在本地已经选择了删除，那么 UPSERT 会被忽略。

客户端执行本地删除时，如果删除对象存在依赖数据，应由客户端业务层负责删除以及生成 tombstone。例如删除 deck 时，依赖该 deck 的 card 和 note 也应删除， 对应数据的 tombstone 也会跟着生成。

同理，如果收到 deck 的删除，那么在此时本地客户端甚至在收到依赖该 deck 的 card 和 note 的 DELETE，就得删除本地对应的 card 和 note，并生成 tombstone，即使后面会重复收到这些依赖对象的 DELETE。

即使后面可能会收到 DELETE 也选择在此时级联删除这些依赖对象的原因是防止，服务端将这些删除延后到下一个 batch 了，这样子中途网络中断就会丢失后续的删除，所以提前删除和生成 tombstone 是更好的选择。


#### 服务端删除策略

服务端删除采用逻辑删除。服务端收到客户端 DELETE 变更时，不物理删除同步实体，而是写入删除标记（delete = true）与服务端为该实体分配的 usn。

### PULLING 状态

PULLING 表示客户端正在从服务端拉取当前同步会话范围内的远端增量数据。

进入 PULLING 的前提是 HandshakeResponse 返回 `NEED_PULL`，客户端持有本次同步会话的 `session_id`，并且服务端 SyncLock 中保存了本轮 Pull 的起点 `client_sync_cursor_usn_at_handshake` 与远端上界 `server_sync_cursor_usn_at_handshake`。

Pull 的同步颗粒度是 batch。服务端按固定实体类型顺序返回变更。每条 `SyncChange` 自带该实体变更对应的服务端 usn。

服务端按下面顺序逐类拉取：Collection -> Deck -> NoteType -> Note -> ProcessingNote -> Card -> ReviewLog。同一个 batch 默认控制在约 64KB；如果为了保证同一个 USN group 不被拆分，batch 可以溢出，但服务端应控制软上限约 128KB。网关 / ConnectRPC 层使用更高的硬上限兜底，例如 1MB。

#### Pull 初始化

服务端创建 / 进入 Pull session 时，根据本轮同步范围判断哪些实体类型有更新：`client_sync_cursor_usn_at_handshake <= usn < server_sync_cursor_usn_at_handshake`。

服务端用一条 SQL 对 7 个固定 entity_type 分别判断是否存在更新。每个分支各自 `LIMIT 1`，避免某个实体类型有大量更新时扫出多余行，同时只产生一次数据库 RTT：

```sql
(SELECT entity_type FROM sync_units WHERE user_id = ? AND entity_type = 1 AND usn >= ? AND usn < ? LIMIT 1)
UNION ALL
(SELECT entity_type FROM sync_units WHERE user_id = ? AND entity_type = 2 AND usn >= ? AND usn < ? LIMIT 1)
UNION ALL
(SELECT entity_type FROM sync_units WHERE user_id = ? AND entity_type = 3 AND usn >= ? AND usn < ? LIMIT 1)
UNION ALL
(SELECT entity_type FROM sync_units WHERE user_id = ? AND entity_type = 4 AND usn >= ? AND usn < ? LIMIT 1)
UNION ALL
(SELECT entity_type FROM sync_units WHERE user_id = ? AND entity_type = 5 AND usn >= ? AND usn < ? LIMIT 1)
UNION ALL
(SELECT entity_type FROM sync_units WHERE user_id = ? AND entity_type = 6 AND usn >= ? AND usn < ? LIMIT 1)
UNION ALL
(SELECT entity_type FROM sync_units WHERE user_id = ? AND entity_type = 7 AND usn >= ? AND usn < ? LIMIT 1);
```

有更新的 entity_type 按固定顺序保存到 Redis session 临时字段，例如 `pull_entity_queue = "1,2,4,6"`。进入 PULLING 时，`sync_cursor_usn = client_sync_cursor_usn_at_handshake`，`expected_batch_seq = 1`。

PULLING 中 `sync_cursor_usn` 复用为当前实体类型内的拉取游标；`pull_entity_queue` 的第一个元素是当前正在拉取的实体类型。切换到下一个 entity_type 时，服务端将 `sync_cursor_usn` 重置为 `client_sync_cursor_usn_at_handshake`；所有 entity_type 拉完后，服务端将 `sync_cursor_usn` 写为 `server_sync_cursor_usn_at_handshake`。

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

`changes` 中每条 `SyncChange` 表示一个实体变更。这里的实体指的是卡片模板、卡片、复习记录等等，详细节见 SyncChange 的定义。PullResponse 中每条 `SyncChange.usn` 必须是服务端已确认的实体 usn，同一实体类型内按 usn 升序返回。

`batch_max_usn` 表示本 batch 内所有 `changes.usn` 的最大值，用于让客户端粗略地校验，避免某次迭代服务端有问题。正常情况下 `batch_max_usn == max(changes.usn)`。

`ChangeOp = UPSERT` 时必须携带 `payload`

`ChangeOp = DELETE` 时不携带 payload，但必须携带 `deleted_at`，客户端根据 `entity_id`、`entity_type` 和 `deleted_at` 按删除同步策略处理本地实体和 tombstone。


#### Pull 本地未同步变更处理

Pull 阶段服务端只负责返回当前同步会话范围内的远端增量，不判断客户端数据是否冲突。客户端收到 PullResponse 后只暂存 `changes`，不要边收边应用到本地业务表。

客户端确认 Pull 全部 batch 完成后，再在本地事务中按服务端返回顺序统一应用暂存的 `changes`。应用远端变更前，必须检查同一 `entity_type` 和 `entity_id` 是否存在本地未同步变更。本地未同步变更包括：

1. 本地实体 `unit.usn = -1` 的 UPSERT。
2. 本地 tombstone 中待上传的 DELETE。

如果本地没有未同步变更，则客户端正常应用远端 `SyncChange`。

如果本地存在 `unit.usn = -1` 的未同步 UPSERT，则客户端按下面规则处理远端 `SyncChange`：

- 若服务端该实体的 `change.usn <= client_sync_cursor_usn`，表示客户端本地修改是基于已同步到本地的服务端版本产生的，所以不该应用这条 `SyncChange`，而是保留本地 `unit.usn = -1`，Pull 完成后在 PUSHING 阶段上传本地变更。
- 若服务端该实体的 `change.usn > client_sync_cursor_usn`，表示服务端在客户端本地修改基线之后也发生了更新，属于并发修改冲突；客户端采用 LWW（Last Write Wins）的策略处理。

LWW 比较客户端本地未同步变更的对象更新时间与远端 `SyncChange` 的对象更新时间。若客户端时间戳更加新，则不应用远端 `SyncChange`，保留本地 `unit.usn = -1` 等待 PUSHING。若`SyncChange`的时间戳更加新，则应用 `SyncChange`，并覆盖本地未同步变更。

对象更新时间由各实体类型的业务更新时间字段决定，如果是删除的对象则由本地 tombstone 使用 `deleted_at` 作为删除变更时间。客户端必须保证进入 PUSHING 阶段时仍然保留的 `unit.usn = -1` 或 tombstone 都是需要上传到服务端的最终本地变更。

#### 客户端处理流程

1. 客户端进入 PULLING 后，从 `batch_seq = 1` 开始发送 PullRequest。
2. 客户端收到 PullResponse 后，校验 `response.batch_seq == request.batch_seq`。
3. 客户端计算本 batch 内最大 `change.usn`，并校验其等于 `response.batch_max_usn`；不一致时视为协议错误，本次同步失败。
4. 客户端暂存本 batch 的全部 `changes`，不要立即应用到本地业务表。
5. 如果 `last_batch = false`，客户端发送下一个 `batch_seq + 1` 的 PullRequest。
6. 如果 `last_batch = true`，表示本轮 Pull 范围内的远端增量已全部收到。客户端开启一个本地 SQLite 事务，按服务端返回顺序处理暂存的全部 `changes`。应用每条变更前，先按删除同步策略与本地未同步变更处理规则判断是否应用。
7. 同一个事务内，远端 UPSERT / DELETE 按删除同步策略落库；全部变更应用成功后，客户端将 `collection.sync_cursor_usn` 推进到本次会话的 `server_sync_cursor_usn`。
8. Pull 完成后，服务端已进入 AWAITING_PUSH_OR_FINISH。如果客户端本地仍存在待上传变更，则发送 PushRequest。否则，进入 FINISHING，调用 FinishSync 释放 session / SyncLock。


#### 服务端处理规则

1. 服务端收到 PullRequest 后，先校验 `session_id` 是否存在、是否匹配当前用户 session、SyncLock 是否仍在 PULLING 状态、`request.batch_seq == SyncLock.expected_batch_seq`；如果校验失败返回 ConnectRPC `FailedPrecondition`。
2. 校验通过后，服务端先将 `expected_batch_seq` 递增 1 并续期 session，用于占住当前 batch 序号，过滤重复请求。
3. 服务端读取 `current_entity_type = pull_entity_queue[0]`、`cursor = sync_cursor_usn`、`upper = server_sync_cursor_usn_at_handshake`。
4. 服务端从当前实体类型中拉取 `entity_type = current_entity_type`、`usn >= cursor`、`usn < upper` 的下一批变更，并按当前实体类型对应业务表批量查询 payload。
5. 服务端组装 `SyncChange`，为每条变更写入该实体变更对应的 usn，计算并写入 `batch_max_usn = max(changes.usn)`。
6. 如果当前 entity_type 未拉完，服务端更新 `sync_cursor_usn = next_pull_cursor_usn`，`last_batch = false`。
7. 如果当前 entity_type 已拉完，但后续还有 entity_type，服务端更新 `pull_entity_queue = remaining_entity_types`、`sync_cursor_usn = client_sync_cursor_usn_at_handshake`，`last_batch = false`。
8. 如果所有 entity_type 都拉完，服务端删除 `pull_entity_queue`，更新 `sync_cursor_usn = server_sync_cursor_usn_at_handshake`、`state = AWAITING_PUSH_OR_FINISH`、`expected_batch_seq = 1`，并设置 `last_batch = true`。
9. 服务端确认 SyncLock 更新成功后，再返回 PullResponse。


#### 中断与超时

Pull 不额外设计 ACK。客户端只有在成功暂存当前 batch 后，才继续请求下一个 batch。

如果 PullRequest 超时、网络中断、客户端崩溃或本地事务失败，客户端不继续猜测当前 batch 状态，直接视为本次同步结束，并丢弃本轮暂存的 Pull changes。下一次重新 Handshake / Pull。


### PUSHING

PUSHING 表示客户端正在把本地 `usn = -1` 的未同步变更上传到服务端。

进入 PUSHING 有两种情况：

1. HandshakeResponse 返回 `NO_REMOTE_CHANGES`，且 `has_local_changes = true`，服务端直接进入 PUSHING，`expected_batch_seq = 1`。
2. PULLING 完成后，服务端进入 AWAITING_PUSH_OR_FINISH；如果客户端本地仍存在待上传变更，则客户端发送第一个 PushRequest，服务端从 AWAITING_PUSH_OR_FINISH 切换到 PUSHING，并从当前 `sync_cursor_usn` 开始为每个上传实体分配 usn。

Push 的同步颗粒度也是 batch。一个 Push batch 对应服务端一次数据库事务，但 usn 不代表 batch；usn 是用户级的全局递增序列号，由服务端分配，且一个实体变更对应一个 usn。客户端上传时本地未同步变更的 `SyncChange.usn` 为 -1，该值表示待上传。

Push 阶段服务端原则上应全部应用客户端上传的变更。变更是否应该覆盖远端数据、是否应放弃本地变更，应在客户端 PULLING 阶段自行处理完成。Pull 完成后仍然保留为 `unit.usn = -1` 或 tombstone 的变更，都表示客户端决定需要推送到服务端的最终本地变更。


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

PushRequest 中每条 `SyncChange.usn` 必须为 `-1`。`usn = -1` 表示客户端本地待上传状态，服务端要为每个实体变更分别分配新的 usn 并写入数据库。


#### PushResponse

```proto
message PushResponse {
  // 当前返回的 batch 序号，等于 request.batch_seq
  int32 batch_seq = 1;

  // 服务端为本 Push batch 内每个实体分配 usn 后返回的确认变更
  // 每条 SyncChange 不携带 payload，但必须携带 entity_id、entity_type、op 和 usn。
  repeated SyncChange changes = 2 [(buf.validate.field).repeated.min_items = 1];
}
```

PushResponse 中的 `changes` 是服务端对 PushRequest 中每条变更的确认结果，不携带 payload，但必须携带 `entity_id`、`entity_type`、`op` 和服务端分配的 `usn`。`op`=`CHANGE_OP_ASSIGN_USN` 表示该响应是回传服务端分配的 usn。

客户端收到 PushResponse 后，根据返回的 `entity_id` 和 `entity_type` 更新本 batch 内已上传实体的本地 `usn`，并将 `collection.sync_cursor_usn` 推进到 `max(response.changes.usn) + 1`。如果本 batch 包含 Collection 变更，collection 表自身的 `usn` 也写为对应返回项的 `usn`。


#### 客户端处理流程

1. 客户端进入 PUSHING 后，从 `batch_seq = 1` 开始发送 PushRequest。
2. 客户端从本地 `usn = -1` 的待上传数据中组装当前 batch，按实体依赖顺序放入 `changes`。
3. 发送 PushRequest 后等待 PushResponse
4. 收到 PushResponse 后，校验 `response.batch_seq == request.batch_seq`。
5. 客户端开启一个本地 SQLite 事务，根据 `response.changes` 将本 batch 内已上传实体的 `usn` 更新为服务端返回的对应 usn，并将 `collection.sync_cursor_usn` 推进到 `max(response.changes.usn) + 1`。
6. 事务提交后，如果本次 `request.last_batch = false`，客户端发送下一个 `batch_seq + 1` 的 PushRequest。
7. 如果本次 `request.last_batch = true`，客户端进入 FINISHING，调用 FinishSync 释放 session / SyncLock。


#### 服务端处理规则

1. 服务端收到 PushRequest 后，Connect validate 先保证 `changes` 非空。
2. 服务端校验 `session_id` 是否存在、是否属于当前用户、SyncLock 是否处于允许 Push 的状态。
3. 允许 Push 的状态包括 PUSHING 和 AWAITING_PUSH_OR_FINISH。
4. 服务端校验 `request.batch_seq == SyncLock.expected_batch_seq`，不匹配时返回 ConnectRPC `FailedPrecondition`。
5. 如果当前状态是 AWAITING_PUSH_OR_FINISH，并且 batch_seq 校验通过，表示客户端选择继续 Push。服务端在处理本次 batch 时先将状态切换为 PUSHING；若本 batch 同时也是最后一个 Push batch，则在数据库事务成功后再切换为 AWAITING_FINISH。
6. 服务端通过 SyncLock 原子领取当前 batch：根据 `changes` 数量从当前 `sync_cursor_usn` 开始预留一段连续 usn，随后将 SyncLock 中的 `sync_cursor_usn` 推进到预留区间上界的下一个 usn、`expected_batch_seq` 递增 1 并续期。
7. 服务端开启数据库事务，将 batch 内所有变更写入数据库，每条 `UPSERT` 实体写入服务端为该实体分配的 usn，`DELETE` 按软删除语义处理并写入该实体对应的 usn。此外，同一事务内顺便更新服务端 `collection.sync_cursor_usn = max(assigned entity usn) + 1`。应用变更时服务端校验：
   - 每条 `SyncChange` 不得为空
   - PushRequest 中每条 `SyncChange.usn` 必须为 `-1`
   - 当 `ChangeOp = UPSERT` 时，确认 payload 与 `entity_type` 匹配
   - 当 `ChangeOp = DELETE` 时，payload 为空，且 `deleted_at` 必须存在。
   - 校验失败返回 ConnectRPC `InvalidArgument`。由于 batch 已经进行过操作，客户端不得重试同一个 batch。
8. 服务端为 batch 内每条请求变更组装一条响应 `SyncChange`：`entity_id` 和 `entity_type` 与请求一致，`op = CHANGE_OP_ASSIGN_USN`，`usn` 为该实体分配的服务端 usn，且不携带 payload。
9. 如果 `request.last_batch = true`，服务端在数据库事务成功后将 SyncLock 状态改为 AWAITING_FINISH；如果 `request.last_batch = false`，SyncLock 保持 PUSHING，等待下一个 batch。
10. 服务端确认数据库事务成功，且必要的 SyncLock 状态更新成功后，返回 PushResponse，其中 `changes` 为本 batch 每个实体的 usn 分配结果。


#### 中断与超时

Push 同样不额外设计 ACK。客户端只有在收到 PushResponse，并成功更新本地 `usn` 和 `collection.sync_cursor_usn` 后，才继续发送下一个 batch。

`last_sync_time` 不在 Push batch 内更新。它表示一次完整同步成功完成的服务端时间，应该在 FinishSync 成功返回后由客户端使用 `server_finished_at` 统一更新。

如果 PushRequest 超时、网络中断、客户端崩溃、本地事务失败，或服务端领取 batch 后数据库事务失败，客户端直接视为本次同步结束。客户端不应该重试同一个 batch，应在下一次重新 Handshake 后继续同步。

同步数据塞进数据库后，即使客户端没收到响应，也不会导致数据不一致。因为后续重新发起同步时，由于没收到响应本地的 usn 会落后服务端，会先 Pull 再 Push，保证数据一致。


### FINISHING 状态

FINISHING 表示同步数据传输已经完成，客户端正在通知服务端结束当前同步会话，释放 SyncLock。

客户端进入 FINISHING 有三种情况：

1. PULLING 完成后，客户端确认本地没有待上传变更。
2. PUSHING 完成后，客户端已经成功处理最后一个 PushResponse。
3. UPLOAD_ALL 完成后，客户端已经成功处理最后一个全量上传 batch 的响应。


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
2. 允许 Finish 的状态包括 AWAITING_PUSH_OR_FINISH 和 AWAITING_FINISH。
3. 校验通过后，服务端释放当前 session / SyncLock。
4. 服务端确认释放成功后，返回 FinishSyncResponse，并将当前服务端时间写入 `server_finished_at`。

如果 `session_id` 不存在、已经过期、属于其他用户或状态不允许 Finish，服务端返回 ConnectRPC `FailedPrecondition`。


#### 中断与超时

如果 FinishSyncRequest 超时、网络中断或客户端崩溃，客户端不能确认服务端是否已经释放 SyncLock。客户端直接视为本次同步结束，不更新 `last_sync_time`。

如果服务端已经释放 SyncLock，但客户端没有收到 FinishSyncResponse，不过此时数据都已经同步完成，唯独本地 `last_sync_time` 没有更新。下一次握手成功时，服务端通过 `HandshakeResponse.server_last_sync_time` 返回权威值，客户端用该值覆盖本地记录。


