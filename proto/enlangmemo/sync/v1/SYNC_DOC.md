# 技术选型

使用 ConnectRPC（一个兼容 gRPC 基于 Protobuf 的 RPC 协议）作为传输基础


# 设计核心原则

主要依靠 usn（update sequence number）字段和批量传输数据完成数据同步
同步颗粒度是 batch（知识单元数量待定），每一个 batch 对应一个数据库事务
服务器负责分配和递增 usn 字段，每 batch 同一个 usn
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
  - NEED_PULL：远端有新增。客户端先 Pull；如果本地也有 usn=-1，则 Pull 后进行对象级合并，再 Push。
  - OVERWRITE_REQUIRED：普通增量同步不安全，让用户选择覆盖方向，具体覆盖实现见“TODO覆盖同步流程”。
3. 开始批量传输数据
4. 数据传输完毕挥手

## 全局 Header

注意所有的 ConnectRPC 操作都带上了全局 header
Authorization: `Bearer ${accessToken}`


## 握手

### 正常握手流程

(一个状态图，在飞书上)

### HandshakeRequest
HandshakeRequest 是发起握手该携带的数据

```proto
message HandshakeRequest {
  string device_id = 1 [(buf.validate.field).string.len = 36]; // 本地标识的设备 UUIDv7，用于区分同一用户的不同设备
  string device_name = 2 [(buf.validate.field).string.max_len = 32]; // 设备展示名
  string collection_id = 3 [(buf.validate.field).string.len = 36]; // 集合 UUIDv7

  // 客户端 collection.sync_cursor_usn，已同步到的 USN 上界 / 下次增量 Pull 起点
  int64 client_sync_cursor_usn = 4 [(buf.validate.field).int64.gte = 0];

  int32 protocol_version = 5; // 同步协议版本
  int32 db_schema_version = 6; // 客户端本地 SQLite schema 版本
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

  // 服务端随机生成的 16 字节 session_id（转为字符串后长度为 32）。
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

（状态机图在飞书上）

### PULLING


### PUSHING