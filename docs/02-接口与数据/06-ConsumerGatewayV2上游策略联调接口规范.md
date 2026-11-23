# ConsumerGateway V2 上游策略联调接口规范

> 定档版本：Consumer API `2.0` / payload schema `2` / wire header `1` / Provisioning Control API `1.0`。本文件面向策略、量化平台和内部 OMS 上游开发者，定义“策略信号 → TradeCore OMS/EMS → BrokerAdapter”的唯一入口，以及账户接入配置的独立控制面。
>
> 当前状态（2026-10-31）：旧 `consumer-uds` 仍为 no-Adapter 性能测试配置；另已部署独立的 `consumer-emt-simulator` 启动入口和真实 EMT 模拟账户链路，UDS 为 `/run/tradecore-emt-simulator/consumer-v2.sock`。已完成一笔真实 Consumer 委托、柜台 `WORKING` 回报与 `PollEvents` 读取，并以 native Journal 确认只报单一次。使用方式及限制见 [08-EMT模拟盘Consumer即用调用说明.md](08-EMT模拟盘Consumer即用调用说明.md)。这不是实盘 READY、完整恢复能力或当前服务的性能 SLA；`SubmitSignal: ACCEPTED` 仍仅代表本地 OMS 接纳。

模拟盘专用授权提供 `SIMULATOR_ACTIVE` 账户状态，表示受控配置允许向指定 EMT 模拟账户提交，而非真实恢复证书通过；其 `reconciliation_status` 为 `NOT_REQUIRED`，不冒充 `VALIDATED`。授权绑定账户、Consumer、Adapter、凭据和地址摘要及签名 namespace，过期或异常后关闭；地址和凭据摘要仅证明与操作员指定配置一致，不能独立证明柜台确为模拟环境。该能力已接入独立服务器启动入口并验证真实模拟委托。模拟盘组合仅下单，`CancelOrder` 返回 `UNAUTHORIZED`。普通 READY 路径的门禁与待完成项不因模拟放行而升级。

2026-10-31 早期隔离准备记录：组合加载接口和虚构单账户 systemd 凭据探针的回归曾为 `39 passed`，当时尚未启动真实会话。后续已完成独立模拟入口部署和真实 Consumer 委托/回报验证，并扩大至 `79 passed`；当前结论以上方状态及 08 调用说明为准。恢复能力仍不因此升级。

2026-10-29 增加独立 `EmtCommittedOutcomeProjectorV1`：仅供已提交的、精确归属的 M1 EMT 订单／成交回报投影到 OMS，并在 OMS Journal 写入后发布 Consumer 事件；账户、交易日、执行、client reference、柜台订单 ID、持仓数量或成交证据不一致时锁存并请求关闭准入。重复成交因当前 OMS 未保存每个成交的原始数量，必须进入权威对账；`CANCEL_ERROR` 不得被当成已撤单。此组件当前**未接入正式服务**；故障关闭依赖调用方提供有效的关闭准入实现，重启回放、重复回报、异步重排、真实账户 binding、撤单错误通知和持续运行均需完整装配及独立验收。

该组件和相邻 Consumer/EMT 定向回归在 Windows CPython 3.10 为 `36 passed`；服务器以 Git 获取 `32aa9bf7835261b39347be54cd3060ad6ebd96b6`，使用项目 CPython 3.10 及离线测试依赖运行相同六个测试文件为 `36 passed`。Linux 测试依赖初次缺失，补充已有离线缓存路径并禁用插件自动加载后通过。后续 `651ac83635afa04e63b6339bf330111689529cff` 增加准入关闭动作失败时的显式错误，Windows 和 Linux 均为 `37 passed`。这仅是组件回归，未启动 EMT、没有真实 UDS 委托和故障恢复演练，也未修改已部署的 1.0 wheel。

2026-10-29 提交侧新增按原始 LSN 读取并验证单个 mmap Journal 完整批次的能力，以及 `verify_emt_committed_admission_v1`：按原始提交位置校验 commit word、CRC、完整批次、canonical recovery seed、OMS 身份、归属和预留资金事实；当前无恢复 seed 的 benchmark Journal 会被明确拒绝。Consumer OMS 增加可选的带 Journal receipt 的单接收者唤醒接口，失败时请求外部关闭准入；它只传递唤醒，不执行 Adapter，实际队列、关闭准入实现与重启 replay 仍待装配。`545cb1c39019323df032a094221524402b1712a1` 在 Windows 和 Linux 的相关定向回归均为 `48 passed`；Linux 通过 Git 拉取隔离源码，未替换正式 1.0 服务。mmap 仍仅提供进程崩溃边界，不承诺掉电持久化。

扩大 Consumer/UDS/Journal 回归时，Linux 首轮为 `149 passed, 4 failed, 2 skipped`：4 项均假定服务端 response EOF 即意味着 client 已关闭和会话已回收。发布版 1.0 的相同测试以相同方式 `4 failed`，两版 `consumer/service_v2.py` SHA-256 均为 `03a9a2975083c5f78588c968f0b74f4f01bc9bacbc87334623630594d2a9c5ae`；代码在完整响应后保留有界读侧 tombstone。`9ea4e15fe19d642506156b1a8ae829a1280ee159` 仅修正测试：客户端收到完整 response/EOF 后主动关闭，再限时验证会话回收，未更改服务端连接行为。Linux 隔离重跑十个测试文件 `153 passed, 2 skipped, 186 subtests passed`；Windows `75 passed, 80 skipped, 186 subtests passed`。跳过项不算通过；这些数据不代替真实 EMT 端到端及故障演练。

上述提交凭证阶段结束时，Consumer→EMT 装配尚未完成；后续代码进度见下节。生产 Consumer 继续为 no-Adapter/DISARMED，不得修改显示状态以替代真实柜台链路。

### 2026-10-29：Consumer→EMT 提交装配代码及隔离测试

已新增 `MmapClientRefJournalV1`，在 owner-private 单宿主控制目录按账户、交易日、Adapter/client scope 确定唯一文件，以 Linux `flock` 防止协作进程同时持有；scope anchor、完整批次与 CRC 校验、缺失文件和损坏尾部均失败关闭。其通过 `VerifiedNamespaceBoundEmtClientOrderIdAllocator` 分配签名范围中的 `order_client_id`；reopen 读取已提交租约并防止引用复用。它不证明跨主机互斥、整机掉电或外部 release anti-rollback；禁止以新空目录/双重删除后的空 Journal 自行推断历史为空。

`EmtConsumerSubmitRoutePreparerV1` 将已验证的 Consumer OMS admission、已激活 namespace、Journal 分配引用、固定 binding 和 route token 组合为预备路由；重复引用、过期、错误 scope 或失败均锁存并请求关闭准入。`EmtConsumerSubmitWorkerV1`、`ManagedEmtConsumerSubmitV1` 和 `MmapEmtSubmitDispositionJournalV1` 以有界单 owner 将提交唤醒交给 EMS，记录本地派发结果；故障或队列/历史容量耗尽不自动重试。预备路由不是 PRE_NATIVE：真正的 `EmtMmapNativeCallJournal` 仍在 SDK 入口前写入 PRE_NATIVE、R1 route ledger 和 CALL_STARTED，随后写 NATIVE_ACCEPTED。最终 Journal 入口新增原始 OMS admission deadline 校验，防止排队后的超时订单因重新计时进入 SDK；该热路径改动尚未进行性能验收。

`build_consumer_emt_submit_uds_v1` 提供单 binding、SO_PEERCRED 的 submit-only 组合。普通路径要求账户/恢复 gate；独立模拟路径要求受控 simulator permit。它不自行加载 SDK/凭据或提供撤单，宿主启动程序负责装配。模拟启动程序已加载实际配置、凭据和会话，且通过真实 UDS 的 `PollEvents` 读到 EMT 回报；当前回放视图仅覆盖该进程，不证明重启后回放。普通路径不得使用测试 lambda 或测试 READY 替代恢复证据。

服务器 Linux Git 提交 `e0452ff3d852759fa66281c61d4bcd0c985ed866` 的隔离回归 `66 passed`：使用真实 AF_UNIX、Consumer/OMS、已签名测试 namespace、文件 Journal、真实 `ExecutionSessionRuntime`/Guard/调度器/EMT Adapter/mutation port，SDK `insertOrder` 是测试替身。测试中 DISARMED 账户不产生 OMS 订单，同一策略业务订单不重复进入替身，且替身调用前确认 PRE_NATIVE 与 CALL_STARTED 已提交。测试用账户 READY、ownership fence 和 release state 均为测试 fixture，不是生产恢复证据，也不是 EMT 模拟盘委托成功；真实 SDK、生产装配、回放/fence 故障矩阵和热路径性能尚未验收。

同一提交在服务器对 Consumer UDS、冷路径、提交链、namespace、OMS 恢复、EMT mutation 与 mmap 压测组件的 21 个测试文件作合并回归：`267 passed, 2 skipped, 186 subtests passed`。复跑 JUnit 为 `/var/tmp/tradecore-consumer-emt-submit-e045-linux.xml`，SHA-256 `d2d310e0f6ecbca1c19ba68e055ff93f0359e46acbdc87d2e57a15cf20e51489`；`/var/tmp` 不属于长期证据归档。该结果只覆盖隔离测试源码及替身，不证明生产服务已装配、模拟柜台已接单、崩溃自动恢复或性能满足生产门槛。

2026-10-29 后续隔离装配：`ManagedEmtCallbackPumpV1` 在线程上 drain 受控 Adapter 回报；持久化成功后由精确 route 证明填补缺失的 execution ID，再投影 OMS，失败关闭准入。`build_provisioned_consumer_emt_adapter_v1` 交叉核验控制面 profile/binding、签名 scope/namespace、映射与发布摘要、回报投影及恢复守卫；凭据只从受控 SecretProvider 短租约获取，且租约内容摘要与 session client ID 必须与已激活 profile/scope 一致。Linux 隔离检出 `c394491e960f9f7726f840d92fba4e31ff6d787d` 的相关回归为 `108 passed`，JUnit `/var/tmp/tradecore-consumer-emt-c394491.xml`，SHA-256 `4b33b6f5ba414dcfbb642aea3ffd4a429357cb455555c82b5ea6fb2ce58cb694`；SDK 仍为替身，scope、READY 和恢复守卫均为测试 fixture。正式服务、真实恢复能力、交易状态和柜台委托未因此变更；`/var/tmp` 不是长期证据库。关闭 callback pump 后，SDK 注销仍可能产生迟到回报，停机不能充作无缺口恢复证明，重启必须先对账。

2026-10-30 控制面隔离改造：新增 PostgreSQL 当前 profile/binding 原子读取、签名 namespace 文件加载及防回退验证、以账户状态/权威恢复证书/风险快照和刷新时效为条件的锁存门禁，以及与 Consumer 提交和 native 调用共用的门禁装配接口。控制面 refresh 在热路径之外执行；恢复证书必须来自独立权威验证，不可凭对象哈希或模拟数据自行认定。Linux 临时独立 PostgreSQL 和相关组件在 `e72f11232948112a0a3a4d7e66420adabda6a754` 为 `126 passed`，JUnit `/var/tmp/tradecore-consumer-emt-e72f112.xml`，SHA-256 `c2bbdf21f52be3a097399b472c43eba5c8a6295f4266a7dfb0507cd257dc40dc`；测试 SDK、风险、账户状态和恢复证明仍是测试数据。临时 PostgreSQL 已停止并清理，不属于生产部署或持久验收证据。

**普通 READY 路径仍未完成**：此前核查的 PostgreSQL 控制面及权威恢复证明尚未闭合；旧 `consumer-uds` 保持 no-Adapter 配置。2026-10-31 新增的独立模拟入口使用 root 受控文件和 systemd 单账户凭据，已加载实际模拟 profile、签名 namespace、会话及 worker，并完成真实 Consumer 模拟委托；该明确授权的模拟路径不使用伪造的恢复证书，也不代表普通 READY 路径已完成。

## 1. 结论与接口分层

上游策略只面对 `ConsumerGateway V2`；不得直接导入 EMT SDK、`EmtBrokerAdapter`、`OrderGateway`、Journal writer、QMT Native Engine 或任一柜台凭据。

```text
策略项目 / 策略 sidecar
  │
  │ 数据面：Linux AF_UNIX + SOCK_STREAM，Consumer V2 fixed frame
  ▼
ConsumerGateway V2
  │  consumer/account/strategy binding + durable idempotency
  ▼
OMS Journal-first admission → EMS → selected BrokerAdapter (EMT / QMT / …)
  │
  └─ ConsumerEvent snapshot / cursor replay → 策略项目

受控配置方
  │
  │ 控制面：HTTPS over HTTP/2 + TLS 1.3 mutual TLS；不进入数据面和热路径
  ▼
Adapter Provisioning Control Plane → SecretProvider / reviewed profile → Adapter activation
```

| 平面 | 预期传输 | 是否可承载交易信号 | 是否可承载账户密码/交易地址 | 当前状态 |
| --- | --- | --- | --- | --- |
| 策略数据面 | **Linux `AF_UNIX + SOCK_STREAM`**，32-byte V2 binary header + canonical UTF-8 JSON | 是 | **否** | 协议、listener、常驻 service/client 已实现；具体 host socket 由部署证据决定。 |
| 本机策略事件 | 同一 UDS 连接上的 V2 response/event frame；cursor replay 是权威路径 | 否 | 否 | snapshot/poll 合同已实现；push event delivery 待实施。 |
| 远程策略访问 | 不允许直接 TCP、HTTP、WebSocket、gRPC 接入交易数据面；应部署同主机受限策略 sidecar，再使用 UDS | 经本机 sidecar | 否 | sidecar 部署形态待实施。 |
| Adapter 配置控制面 | **HTTPS/HTTP/2 + TLS 1.3 双向 mTLS**；严格 RFC 7516 JWE | 否 | 是，但仅限拥有 `ADAPTER_PROVISIONER` 角色的受控客户端 | listener、JWE、approval、profile lifecycle、secret boundary 已实现；真实 host endpoint 仍需部署。 |
| 柜台原生连接 | EMT/QMT 等 Adapter 私有 SDK 通道 | 由 Adapter 负责 | 仅服务账户运行时可读取 | EMT 已有受控运行路径；QMT 尚处 P0 `PROBE`。 |

数据面选择 UDS 而非 TCP/WebSocket 的原因是：策略与交易进程必须同一受控 Linux 主机，服务端可用 `SO_PEERCRED` 取得不可伪造的 UID/GID/PID，并把它映射为 `ConsumerPrincipal`。HTTP、WebSocket 和 TCP 不能替代该本机身份边界；它们也不应成为 EMT 热路径的额外网络跳数。

## 2. 身份、账户、策略与多 Adapter 路由

### 2.1 上游可见身份

策略请求**不得**携带 `consumer_id`、内部 `account_id`、`adapter_instance_id`、柜台账号、`client_id`、柜台订单号或路由 token。服务端在连接建立时由 UDS peer credential 解析 `ConsumerPrincipal`，再以不可变 `ConsumerBindingSnapshotV2` 授权。

一个有效账户 binding 的内部路由键为：

```text
(consumer_id, consumer_account_id) → (account_id, venue_id, adapter_instance_id)
```

`ConsumerAccountBinding` 已包含 `venue_id` 与 `adapter_instance_id`，而 `ConsumerStrategyBinding` 把 `strategy_id` 绑定到该账户 binding。因此同一个上游消费者可以持有多个已批准的 `consumer_account_id`，例如一个对应 EMT、一个对应 QMT；策略仅以 `consumer_account_id` 选择目标。

这刻意禁止策略在单笔 `SubmitSignal` 中临时指定 `EMT`、`QMT`、任意 endpoint 或 adapter instance：该做法会绕过账户 ownership、能力 profile、scope 和恢复边界。若同一柜台账户需要切换 Adapter，必须在控制面 `DISARMED` 状态下停新单、对账、撤销旧 binding、创建新的 binding 并提升 authorization epoch；不允许热路径自动 failover。

### 2.2 `ListAccounts` 是唯一的选择入口

策略启动后必须先调用 `ListAccounts`。每个返回项目包括：

```json
{
  "consumer_account_id": "策略可见的不透明 binding ID",
  "account_display_name": "受限展示名",
  "venue_id": "EMT | QMT | PAPER",
  "capability_profile_id": "当前能力 profile",
  "lifecycle": "DISARMED | READY | …",
  "reconciliation_status": "VALIDATED | REQUIRED | …"
}
```

`adapter_instance_id` 和柜台实际账号不向策略暴露。策略保存选定的 `consumer_account_id`，并将其用于之后所有 `SubmitSignal` 和 snapshot 调用。

## 3. 数据面连接、帧与版本协商

### 3.1 连接规则

待实施的服务必须由 Linux TradeCore 运行账户在受限目录创建绝对 pathname socket，例如由运行配置给出：

```text
<run-root>/consumer-v2.sock
```

目录必须归服务账户所有、不可为 symlink、权限不宽于 `0700`；socket 本身为 `0600`。策略 sidecar 使用同一个获授权 UID/GID 连接。服务器不接受 abstract socket、`SOCK_SEQPACKET`、datagram、TCP 或带 ancillary FD/credential data 的帧。

单连接默认 timeout 为 5 秒、最多处理 1,024 个请求；达到上限、收到半帧超时、未知关键字段、非 request message kind 或认证失败时服务端关闭连接。客户端应重新建立连接并按第 6 节的幂等规则恢复，绝不能凭连接断开推断“未下单”。

### 3.2 固定 32-byte header

所有数据面消息先编码为 canonical JSON（UTF-8、排序 key、紧凑分隔符），再附加以下网络字节序（big-endian）header。不得手写普通 JSON socket 协议。

| 偏移 | 类型 | 值/规则 |
| ---: | --- | --- |
| 0 | `bytes[4]` | magic：`XTC2` |
| 4 | `u8` | header version：`1` |
| 5 | `u8` | message kind：`1=request`、`2=response`、`3=event`、`4=error` |
| 6 | `u16` | flags：必须为 `0` |
| 8 | `u16` | API major：`2` |
| 10 | `u16` | API minor：`0` |
| 12 | `u16` | payload schema version：`2` |
| 14 | `u16` | reserved：必须为 `0` |
| 16 | `u64` | connection-local correlation ID；请求/响应保持相同值 |
| 24 | `u32` | payload bytes length，最大 `1,048,576` |
| 28 | `u32` | payload CRC-32 |

完整 frame 最大 `1,048,608` bytes。客户端应复用项目的 `encode_frame()` / `decode_frame()`；不得假设 `recv()` 恰好返回一个完整 frame。

```python
from xuanling_tradecore.consumer.wire_v2 import MessageKind, encode_frame

frame = encode_frame(MessageKind.REQUEST, correlation_id=1, payload=request_payload)
unix_socket.sendall(frame)
```

正式联调前会交付独立 client SDK，负责分段读写、correlation ID、schema hash 校验、超时、重连和 cursor checkpoint；上游项目不应复制服务端 decoder。

### 3.3 合同文件与兼容性

客户端与服务端必须绑定同一 release 的下列资产：

```text
contracts/consumer/v2/schemas/request.schema.json
contracts/consumer/v2/schemas/response.schema.json
contracts/consumer/v2/schemas/event.schema.json
contracts/consumer/v2/schemas/error.schema.json
contracts/consumer/v2/schema-hashes.json
contracts/consumer/v2/compatibility-matrix.json
```

当前 V2 reader/writer 仅与 `2.0` 兼容。V1 framing/cursor 必须返回 `UPGRADE_REQUIRED`；未来 V3 event 不允许静默跳过或推进 cursor，必须返回 `EVENT_SCHEMA_INCOMPATIBLE`。

## 4. 请求接口

每个请求通用外层为：

```json
{
  "message_type": "request",
  "schema_version": 2,
  "operation": "操作名",
  "request_id": "1..128 字符的策略请求 ID",
  "payload": {}
}
```

`request_id` 是传输/回执关联 ID，推荐 UUIDv7；对不确定结果的重试必须保持不变。它不是业务幂等主键，业务幂等由 `strategy_id + strategy_order_id + canonical intent` 决定。

| 操作 | payload | 成功结果 | 当前实现状态 |
| --- | --- | --- | --- |
| `ListAccounts` | `{}` | 授权账户列表 | Gateway 合同已实现。 |
| `SubmitSignal` | 见 4.1 | `IntentReceipt`，状态 `ACCEPTED` | Gateway/OMS operation 已实现；到 EMT Adapter 的运行时 composition 待实施。 |
| `CancelOrder` | `oms_order_id`、`reason` | cancel `IntentReceipt`，状态 `ACCEPTED` | OMS operation 合同已实现；实际 Adapter cancel composition 待实施。 |
| `GetOrder` | `oms_order_id` | 当前可见 order event | 合同已实现。 |
| `GetOrderAuditTrail` | `oms_order_id` | durable audit pointers | 合同已实现。 |
| `GetAccountOrderSnapshot` | `consumer_account_id`、可选 `reset_context` | order snapshot + `next_cursor` | 合同已实现；真实 projection composition 待实施。 |
| `PollEvents` | `cursor`、`limit` | 固定 watermark 的有序 event page | 合同已实现。 |
| `CreateReplayPartitions` | `consumer_account_id`、`partition_count(1..64)` | 固定快照的相邻区间、事件数与独立签名 cursor | 当前无 Adapter UDS target 已实现；其他 composition 需装配 partition reader。 |
| `SubscribeOrderEvents` | `cursor` | subscription acceptance | 注册合同已实现；实际 server push 待实施。 |

### 4.1 `SubmitSignal`

```json
{
  "message_type": "request",
  "schema_version": 2,
  "operation": "SubmitSignal",
  "request_id": "018f0e7e-0000-7000-8000-000000000001",
  "payload": {
    "consumer_account_id": "从 ListAccounts 取得",
    "strategy_id": "预登记且已授权的稳定策略 ID",
    "strategy_order_id": "策略业务订单幂等键",
    "trading_day": "YYYY-MM-DD",
    "symbol": "策略标的标识",
    "market": "已登记市场标识",
    "side": "BUY 或映射 profile 允许的值",
    "price_type": "LIMIT 或映射 profile 允许的值",
    "business_type": "CASH 或映射 profile 允许的值",
    "quantity": 100,
    "protection_price_ticks": 0,
    "limit_price_ticks": 12345,
    "execution_parameters": {
      "algorithm": "可选的预登记执行参数"
    }
  }
}
```

| 字段 | 规则 |
| --- | --- |
| `consumer_account_id` | 必须来自当前会话 `ListAccounts` 的 active binding，长度 1–128。 |
| `strategy_id` | 必须预登记到该 binding；策略不能传另一个消费者的 ID。 |
| `strategy_order_id` | 同一业务订单跨重试、重启、网络断开保持不变；同 key 但 canonical intent 不同为 `IDEMPOTENCY_CONFLICT`/结构化拒绝，禁止生成第二笔订单。 |
| `trading_day` | `YYYY-MM-DD`；生产以服务端 `GetTradingDay()`/trading-day epoch 校验为准。 |
| `symbol`、`market`、`side`、`price_type`、`business_type` | 仅携带策略语义；Adapter mapping/profile 决定 EMT/QMT 原生枚举，策略不能直接传原生整数。 |
| `quantity` | 正整数。 |
| `*_price_ticks` | 整数 tick，禁止浮点数；`limit_price_ticks` 可为 `null`，但 price type/profile 必须允许。 |
| `execution_parameters` | 最多 32 项；key 1–128 字符，value 为最多 512 字符的字符串；不得夹带密码、token、地址或自定义柜台参数。 |

成功回执：

```json
{
  "message_type": "response",
  "schema_version": 2,
  "request_id": "018f0e7e-0000-7000-8000-000000000001",
  "status": "ACCEPTED",
  "payload": {
    "consumer_request_id": "与 request_id 相同",
    "accepted_event_id": "本地已耐久事实 ID",
    "oms_order_id": "OMS 订单 ID",
    "execution_id": "执行 ID",
    "journal_id": "Journal ID",
    "journal_lsn": 123
  }
}
```

`ACCEPTED` 只表示策略信号已通过授权并产生 Journal-first 准入事实；它**不**表示 EMT `InsertOrder()` 已返回、柜台已受理、订单已报、已成交或可撤。后续状态只能由 event/snapshot 获得。

### 4.2 `CancelOrder`、查询和审计

```json
{"operation":"CancelOrder","payload":{"oms_order_id":"OMS 订单 ID","reason":"策略可审计撤单理由"}}
{"operation":"GetOrder","payload":{"oms_order_id":"OMS 订单 ID"}}
{"operation":"GetOrderAuditTrail","payload":{"oms_order_id":"OMS 订单 ID"}}
```

撤单只接受由当前 consumer binding 精确归属的 `oms_order_id`。任何未证明的柜台订单、其他消费者订单、仅靠 PostgreSQL 猜测的订单或失效 session 都必须返回非泄漏性 `NOT_FOUND`/`UNAUTHORIZED`，而不是尝试柜台撤单。

### 4.3 snapshot、cursor replay 与 live subscription

策略启动、cursor 过期或收到 stream reset 时执行：

1. `ListAccounts`，确认目标 binding 可用；
2. `GetAccountOrderSnapshot`，保存响应中的 snapshot 和 `next_cursor`；
3. 循环 `PollEvents(cursor, limit)`，按 `position` 递增处理，以 `event_id` 去重；
4. 仅在自身状态成功落库后，保存 response 中的 `next_cursor`；
5. 重连或 cursor 失效时重复 snapshot，不得猜测遗漏事件；
6. 未来启用 push 后，先 cursor catch-up，再 `SubscribeOrderEvents`；push 只能加速通知，cursor replay 始终是权威恢复路径。

`PollEvents.limit` 范围是 `1..10000`。cursor 是签名 capability，绑定 consumer、consumer account、Journal、stream generation、authorization epoch、expiry 和 watermark；策略不得拼接、解码修改或跨账户复用。

并行回放时先调用一次 `CreateReplayPartitions`，每个 worker 独占一个返回的 cursor，
后续沿各自 next_cursor 前进；固定区间为 `(after_position, high_watermark_position]`。
这提供有界读取 capability，不是自动领取的消费组租约。事件处理和 next_cursor 的持久化
checkpoint 必须由客户端原子提交。完整请求/响应与 UDS-S2-5K 口径见
[签名分片联调说明](../04-测试验收与性能/04-OMS性能与容量/14-PollEvents签名分片游标与UDS-S2-5K联调说明.md)。

### 4.4 查询与回放响应字段

所有成功响应共用 `message_type=response`、`schema_version=2`、原始 `request_id`、`status=COMPLETED|ACCEPTED` 和 `payload`。以下为 operation-specific `payload`；字段未列出即不可由客户端依赖。

| 操作 | `payload` |
| --- | --- |
| `ListAccounts` | `accounts[]`，每项为 `consumer_account_id`、`account_display_name`、`venue_id`、`capability_profile_id`、`lifecycle`、`reconciliation_status`。 |
| `SubmitSignal` / `CancelOrder` | `consumer_request_id`、`accepted_event_id`、`oms_order_id`、`execution_id`、`journal_id`、`journal_lsn`。 |
| `GetOrder` | `order`：一条不带 cursor `position` 的 ORDER event 视图，包含 `event_id`、`event_kind`、`oms_order_id`、`execution_id`、`order_state`、`order_revision`。 |
| `GetOrderAuditTrail` | `audit_trail[]`，每项为 `audit_id`、`oms_order_id`、`execution_id`、`journal_id`、`journal_lsn`、`action`。 |
| `GetAccountOrderSnapshot` | `consumer_account_id`、`snapshot_position`、`orders[]`、`next_cursor`。该 cursor 是此一致快照后的唯一重启点。 |
| `PollEvents` | `events[]`、`high_watermark_position`、`has_more`、`next_cursor`。每项 event 必有 `position`，按数值 position 升序。 |
| `SubscribeOrderEvents` | `cursor`，状态为 `ACCEPTED`。当前不代表 server 已开始推送，必须继续 PollEvents 直至 push service 上线并通过验收。 |

策略每次收到 `next_cursor` 时，应把“业务状态更改、去重 event_id、cursor 更新”放入自身同一个本地事务。处理失败时不保存新 cursor；下次以旧 cursor 重读是预期的至少一次交付行为。

## 5. 响应、事件与错误

### 5.1 事件

event frame 的共同字段为：

```json
{
  "message_type": "event",
  "schema_version": 2,
  "event_id": "稳定 consumer event ID",
  "event_kind": "ORDER | ACCOUNT_STATUS | ACCOUNT_RISK",
  "position": "ce2_…",
  "payload": {}
}
```

| `event_kind` | payload 关键字段 | 含义 |
| --- | --- | --- |
| `ORDER` | `oms_order_id`、`execution_id`、`order_state`、`order_revision` | OMS 对一个可归属订单的可见状态。 |
| `ACCOUNT_STATUS` | `lifecycle`、`reconciliation_status`、`gates` | 账户是否仍允许新单；`READY` 外的状态不得被策略忽略。 |
| `ACCOUNT_RISK` | `risk_snapshot_id`、`error_category`、`summary` | 风控/恢复/可见风险摘要，不含原生凭据或回包。 |

事件不是成交保证；最终业务状态由该订单后续 `ORDER` event、完整 callback 与权威恢复结论共同决定。

### 5.2 错误与重试

| 类别 | 典型 code | 客户端动作 |
| --- | --- | --- |
| framing/version | `FRAME_INCOMPLETE`、`BAD_MAGIC`、`UNSUPPORTED_*`、`PAYLOAD_CRC_MISMATCH`、`INVALID_*`、`UNKNOWN_CRITICAL_FIELD` | 修复 client/schema 后再建立连接；不得自动降级协议。 |
| 请求语义 | `SCHEMA_VALIDATION_FAILED`、`EVENT_SCHEMA_INCOMPATIBLE` | 修复请求；不得原样无限重试。 |
| 授权/归属 | `UNAUTHORIZED`、`NOT_FOUND` | 重新 `ListAccounts` / 刷新 binding；不得替换为别的账户或策略 ID。 |
| cursor/replay | `CURSOR_INVALID`、`CURSOR_EXPIRED`、`JOURNAL_GAP`、`STREAM_RESET_MISMATCH`、`CURSOR_RECOVERY_UNAVAILABLE` | 执行 snapshot + 新 cursor；不推进本地 checkpoint。 |
| 服务/容量 | `RATE_LIMITED`、`INTERNAL_ERROR`、`AUDIT_EVIDENCE_UNAVAILABLE` | 只在 `retryable=true` 时使用**同一** request ID 和同一 strategy order ID 有界重试；否则停止策略该条信号并告警。 |

连接在 header/schema 失败时可能直接关闭而没有 error frame；这不代表数据面没有接收请求。对于已发送而未收到 response 的 `SubmitSignal`，必须带同一 `request_id` 和同一 `strategy_order_id` 重试，或通过 snapshot/event 查证，绝不能生成新业务订单 ID。

## 6. 多 Adapter 支持检查

| 要求 | 当前设计结论 | 现状 |
| --- | --- | --- |
| 一个 consumer 选择多个 Adapter | **满足模型要求。** 一个 consumer 可有多个 `ConsumerAccountBinding`，每个 binding 固定 `venue_id + adapter_instance_id`。 | 代码已实现 binding 验证。 |
| 策略安全选择目标 Adapter | **满足设计。** 用 `ListAccounts` 返回的 `consumer_account_id` 选择，而不是传自由 adapter 字段。 | V2 schema/dispatcher 已实现。 |
| 一个信号动态指定任意 Adapter / endpoint | **不允许且不应实现。** 会绕过 scope、capability、恢复和账户 ownership。 | 已 fail-closed 设计。 |
| Consumer V2 真正路由至多个 live Adapter | **尚不满足。** `OrderBookConsumerOperationsV2` 明确不调用 Adapter；无常驻 runtime composition。 | 待实施。 |
| EMT | Adapter/执行链已有 EMT-only 实现与验收，但尚未由 Consumer V2 UDS 常驻服务暴露。 | 待 Consumer runtime composition。 |
| QMT | QMT Edge 仅完成 Windows `PROBE_READY_DISARMED`；不能从 Consumer V2 交易。 | P0 未完成。 |
| PaperBroker | 已按项目范围跳过。 | 不可用。 |

因此，当前设计**可以承载多 Adapter 的正确数据模型**，但当前上游策略还不能通过一个已部署 Consumer 服务实际选择并调用多个 Adapter。实现时必须先完成 EMT Consumer service，再按 QMT P0–P4 和 capability gate 加入 QMT binding；不得用策略侧直连 SDK 作为临时替代。

## 7. 账户、密码和交易地址：独立 Adapter Provisioning Control Plane

### 7.1 不进入 `SubmitSignal`

当前 Consumer V2 request schema 没有、也不会增加以下字段。此规则适用于 request、response、event、cursor、trace、Journal、异常文本和性能指标：

```text
broker_account / password / password_type / token / client_id
trading_host / trading_port / SDK path / adapter endpoint
adapter_instance_id / adapter_client_scope / native route token
```

这是现有设计的有意安全边界：这些字段进入策略 data plane 会被 Journal、性能 trace、重试队列、错误日志或其他策略客户端接触，且允许策略在热路径改变柜台身份。当前运行规范也要求密码不写入生产配置、Journal、PostgreSQL、Git、命令行或环境转储。

所以“上游消费者可以传输账号、密码和交易地址”只能理解为：**经独立、受控、非热路径的配置面由拥有配置角色的系统提交**，而不是普通策略 `SubmitSignal` 的能力。当前仓库尚无该控制面服务，因此该要求目前不满足；下列是本项目定档后的唯一可接受实现。

### 7.2 预期控制面接口（待实施）

| 接口 | 方法与传输 | 权限 | 作用 |
| --- | --- | --- | --- |
| 获取加密收件人 | `GET /control/v1/credential-recipients/{target}`，HTTPS HTTP/2、TLS 1.3 mTLS | `ADAPTER_PROVISIONER` | 返回受控 server public key、`kid`、算法、受众和有效期；不返回任何 secret。 |
| 准备 Adapter profile | `POST /control/v1/adapter-profiles:prepare`，同上 | `ADAPTER_PROVISIONER` | 校验非敏感 identity；解密完整 credential/connection envelope 后写入目标 SecretProvider，返回不含 secret 的 digest 和 profile revision。 |
| 验证 profile | `POST /control/v1/adapter-profiles/{id}:validate`，同上 | `ADAPTER_PROVISIONER + APPROVER` | 只允许 `PREPARED` profile；在 `DISARMED` 下运行只读 SDK/capability probe；不发送订单。 |
| 激活 binding | `POST /control/v1/consumer-bindings:activate`，同上 | `APPROVER` | 仅允许 `VALIDATED` profile；原子生成/轮换 binding snapshot 和 authorization epoch；不自动使账户 `READY`。 |
| 撤销/销毁 | `POST /control/v1/adapter-profiles/{id}:retire`，同上 | `ADAPTER_PROVISIONER + APPROVER` | 停新单、撤销 binding、销毁 SecretProvider reference 并保留不可逆审计摘要。 |

控制面不使用 WebSocket；配置是低频、可审计、需要 request/response 确认的操作。数据面仍保持 UDS。控制面 service 也不得复用策略 UDS listener、EMT native caller 或任何 C0–C7 线程。

### 7.3 凭据安全 envelope（待实施）

上游配置客户端先取得**实际 secret 所在主机**的有效 recipient key，然后以标准 RFC 7516 JWE（`RSA-OAEP-256` + `A256GCM`，携带 server `kid`）封装以下最小 JSON。传输仍必须使用 TLS 1.3 mTLS；JWE 用于防止中间日志、代理和持久队列看到明文。

JWE protected header 必须包含 `alg`、`enc`、`kid`、`typ=tradecore-broker-credential+jwe`、`cty=application/json`；明文必须包含以下 anti-replay claims：`jti`（UUIDv7）、`iss`（mTLS principal ID）、`aud`（recipient target）、`iat`、`nbf`、`exp`。`exp - iat` 不得超过 5 分钟，`jti` 在有效期内只能被接收一次。控制面校验 mTLS principal 与 `iss` 一致、target 与 `aud` 一致、外层 `profile_id/adapter_kind/adapter_instance_id` 与密文一致后才可写入 SecretProvider。

```json
{
  "credential_schema": "TradeCoreBrokerCredentialV1",
  "jti": "一次性 UUIDv7",
  "iss": "配置客户端 mTLS principal ID",
  "aud": "linux-emt-secret-provider | windows-qmt-edge-secret-provider",
  "iat": "RFC 3339 UTC",
  "nbf": "RFC 3339 UTC",
  "exp": "RFC 3339 UTC，最长五分钟",
  "profile_id": "非敏感 profile ID",
  "adapter_kind": "EMT",
  "adapter_instance_id": "不可复用实例 ID",
  "credential": {
    "broker_account": "仅在 JWE 明文中出现",
    "password": "仅在 JWE 明文中出现",
    "client_id": "仅在 JWE 明文中出现"
  },
  "connection": {
    "kind": "BROKER_TCP",
    "host": "交易地址",
    "port": 0,
    "server_name": null
  },
  "not_after": "配置过期时间"
}
```

服务端只在受限进程/受限内存中解密，创建 `SecretProvider` 受控 secret reference。Python 对不可变 `str`/`bytes` 不能作绝对内存清零承诺，因此实现必须尽量使用短生命周期可变缓冲、禁止把明文转入异常、日志、repr、metrics、trace、子进程参数或环境变量，并通过账户进程隔离和 OS ACL 限制残余暴露面；不得宣称“Python 已保证零化”。持久化、审计、响应、日志和指标只可记录 `profile_id`、adapter kind、endpoint digest、credential digest、key ID、版本、操作者、审批引用和时间。不得回显账号、密码、明文 endpoint、JWE 明文或原始 SDK 配置。

对 QMT，QMT 登录凭据必须只写入 Windows Edge/QMT 主机的 SecretProvider；Linux Consumer 控制面仅可保存一个受限 QMT Edge profile reference，绝不把 QMT 密码转入 Linux 策略或 mTLS command stream。对 EMT，凭据仅由该 EMT Linux Adapter 服务账户在启动/受控轮换时读取。

### 7.4 `adapter-profiles:prepare` 的预期非敏感请求结构（待实施）

控制面请求的外层可审计但不含任何 connection 或明文凭据；`credential_envelope` 是唯一可以携带柜台账号、密码、client ID、交易地址和端口的字段，且只能是完整 JWE 字符串。任何出现 `password`、`token`、`broker_account`、`host`、`port` 明文字段的请求一律拒绝并记录无敏感内容的安全事件。

```json
{
  "request_id": "控制面 UUIDv7",
  "profile_id": "上游定义的非敏感 profile ID",
  "adapter_kind": "EMT | QMT",
  "adapter_instance_id": "不可复用实例 ID",
  "account_alias": "不等于柜台账号的受限别名",
  "credential_envelope": "JWE compact serialization",
  "expected_credential_kid": "从 credential-recipients 获取",
  "not_after": "RFC 3339 expiry",
  "approval_reference": "受控审批引用"
}
```

服务端响应只能含 `profile_id`、`profile_revision`、`adapter_kind`、`endpoint_digest`、`credential_digest`、`state=PREPARED|VALIDATED|REJECTED`、`approval_required` 和不含敏感信息的错误 code。验证与激活必须以新的配置 revision 产生新的 binding snapshot；普通策略 session 不具备读取、更新、替换或删除 profile 的任何权限。

### 7.5 profile 状态机、幂等与版本语义

`profile_id` 是稳定的非敏感逻辑名称；每次成功 `prepare` 都生成不可变、单调递增的 `profile_revision` 和一次性 `adapter_instance_id`。同一 `(profile_id, profile_revision)` 的相同 `request_id` 重试必须返回同一脱敏响应；同一个 `request_id` 的不同 payload digest 必须以 `CONTROL_IDEMPOTENCY_CONFLICT` 拒绝。不得通过覆盖旧 revision 变更密码、交易地址、账号或 Adapter kind。

```text
ABSENT → PREPARED → VALIDATED → ACTIVE_DISARMED → RETIRING → RETIRED
                   ↘ REJECTED
```

- `PREPARED`：密文已通过 anti-replay 校验，SecretProvider 仅保存受限 reference；尚未加载 SDK、连接柜台或对策略可见。
- `VALIDATED`：只读验证、能力矩阵、scope/ownership、release/bundle 与 host-health 证据均通过；任何缺口回到 `REJECTED`，不得降级为可交易。
- `ACTIVE_DISARMED`：binding snapshot 已原子发布，可由 `ListAccounts` 作为不可交易或受控状态展示；仍不表示 `READY` 或自动登录/下单。
- `RETIRING`：拒绝新 signal，旧 binding/订单/对账证据保留；working、unknown、未归属外部订单、未释放保留或未完成回放时不得进入 `RETIRED`。
- `RETIRED`：SecretProvider reference 已销毁，历史 identity/审计摘要只读保留；旧 `adapter_instance_id` 永不复用。

控制面请求与数据面请求使用不同的 `request_id` namespace、日志 logger、审计表和速率预算。控制面不得使用数据面 UDS 端口，也不得绑定 EMS native caller、callback 或 Journal worker CPU；控制面故障只能使新配置/轮换失败，不能阻塞已激活 Adapter 的 C0–C7 热路径。

## 8. 上游开发顺序与验收

上游策略开发可先按 V2 schema 实现 request model、稳定幂等键、response/event reducer 和 cursor checkpoint，但在服务端交付前只能跑 fixture/golden vector，不能假设有真实 socket 或 EMT side effect。

| 顺序 | 上游动作 | 服务端必须提供 | 验收标准 |
| ---: | --- | --- | --- |
| 1 | 固定 Consumer V2 contract assets | 同 revision schema hashes / golden vectors | client 能编码/解码完整 frame，拒绝版本/CRC/长度错误。 |
| 2 | 实现 `ListAccounts → snapshot → PollEvents` state reducer | UDS service + peer UID/GID binding | 无跨 consumer/account event；checkpoint 仅在本地事务后前移。 |
| 3 | 实现 `SubmitSignal` 幂等重试 | OMS/EMS/EMT composition | 同 strategy order 重试零重复 native call；回执与 Journal 证据可追溯。 |
| 4 | 实现 cancel/query/audit | authorized OMS operation port | 不可归属订单零撤单、零信息泄露。 |
| 5 | 加入多个 binding 的账户选择 UI/策略配置 | Adapter registry + active binding snapshot | 只能选被授权 binding；切换/撤销立即使旧 session epoch 失效。 |
| 6 | 仅配置系统接入 Provisioning API | mTLS/JWE/SecretProvider/control approval | 明文 secret 零日志、零 Git、零 Journal；每次变更均 `DISARMED` 并有审计。 |
| 7 | QMT/Paper 等 Adapter 扩展 | 各自 capability/recovery/performance gate | 未通过 gate 的 binding 不出现在可交易账户列表。 |

## 9. 当前设计核验总结

| 用户目标 | 是否满足 | 结论 |
| --- | --- | --- |
| 上游策略有统一的 EMT/OMS/EMS 调用标准 | **合同与 UDS service/client 满足** | `ListAccounts`/snapshot/PollEvents/SubmitSignal 的 Consumer V2 合同及本机客户端已实现；Adapter runtime 仍只接受已批准 binding。 |
| 明确 TCP、WebSocket 等调用方式 | **满足** | 热路径唯一为本机 UDS；TCP/WS 不支持。配置面为独立 HTTPS HTTP/2 mTLS。 |
| 上游可调用多个 Adapter | **binding/registry 满足** | 通过多个已授权 binding 选择，不暴露自由 adapter/host routing；每个 host 的 capability gate 仍独立验收。 |
| 上游可传账号、密码和交易地址 | **仅控制面满足，data plane 永不满足** | 仅有 `ADAPTER_PROVISIONER` 的 mTLS+JWE 控制面可提交；普通策略没有此权限。 |
| 账号密码不会影响热路径、不会泄漏至策略事件 | **设计满足** | 凭据与 endpoint 严格排除在 `SubmitSignal`/ConsumerEvent/Journal/PG/日志之外。 |

当前下一项工作是按 `deploy/linux-control-plane/README.md` 以受控 factory 部署控制面，并分别提供 EMT/QMT 的 host capability/recovery/性能证据；不存在安全且受支持的“策略项目直接传密码并调用 EMT”的接口。

## 10. 持续事件消费的签名后继分片

`CreateReplayPartitions`冻结的是健康读模型已经验证、且不超过本次捕获的Journal提交边界的事件前缀。异步projector尚未追到不断增长的live head时，可返回该已验证前缀；不得把它冒充最新全量或恢复权威证书。某轮空后缀只表示这个已发布cut内暂无后继事件，不代表源Journal/柜台没有新事件；消费者保留原exhausted cursor，稍后继续申请。规划和后继读取仍拒绝失效health、错scope/epoch/generation、过期签名及越界origin；主动PAUSED时不得签发新plan，最终交付也复核该操作上下文，已有签名冻结范围仍按原规则可读。`GetOrder`等要求当前性的查询仍保留live-head minimum检查。性能验收仍须独立核对生产量/消费量及projection/consumer lag，不因允许前缀续读而降低门槛。

支持后继规划的读模型可接受 `CreateReplayPartitions.payload.after_cursor`（可选字符串，1–4096字符）。不传时保持原有从范围起点规划的行为；传入时只接受**当前stream、同consumer/account/授权epoch、尚未过期且已到达签名high watermark**的cursor。只修改token里的LSN、传入未消费完的起始token、旧stream或过期token均拒绝；不接受裸 `after_lsn`。

推荐调用顺序：

1. `CreateReplayPartitions` 获取官方独立分片，每片独立 `PollEvents`。
2. 每片事件落入消费者本地事务后，持久化服务返回的 `next_cursor`；全部分片完成后，保留最后一片已耗尽的 `next_cursor`。
3. 请求 `CreateReplayPartitions`：`{"consumer_account_id":"已授权别名","partition_count":4,"after_cursor":"上一步保存的原样token"}`。服务只对该签名边界之后的新事件规划，不重扫为重复消费吞吐。
4. 如果新计划为空，保留原token，按退避重试，不把响应空high watermark覆盖checkpoint；有新分片时验证首片after_position等于上轮终点，其余相邻边界连续。签名cursor在本地进度存储中视为敏感能力，不进入公开结果文件。

该token证明可读取范围，不是分布式消费租约，也不能证明其他分片已落盘；全部分片checkpoint一致性由消费者负责。实现不支持后继规划时明确返回 `CURSOR_RECOVERY_UNAVAILABLE`，不可退化为自动从BEGIN重读并重复计数。读worker完成后还会重新校验origin及输出token，读取期间过期/授权撤销不得交付成功。V2 framing不变；本次为可选字段扩展，使用同revision的schema-hashes和golden assets联调。
