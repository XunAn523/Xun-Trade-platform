# QMT Windows 边缘适配与 TradeCore `QmtBrokerAdapter` 定档

> 状态：**部分实施；P0 的门禁/ABI 代码已实现，真实 QMT 探针未开始。** 2026-09-16 已按 QMT 官方“内置 Python / 快速交易”语义重定档：生产目标是 QMT 内置 Python 的原生执行引擎，而不是外部进程定时扫描文件后再转发。本文件是 QMT 生产适配的唯一设计基线。它不改变当前 EMT-only 的已验收结论，也不将 QMT fake profile、Windows 本机网关或任意网络连通性表述为 QMT 可交易、可恢复或 `READY`。

> 范围：Linux 上的 TradeCore OMS/EMS 与 Windows 上的大 QMT 进程通过一条专用直连链路协作。QMT 是与 EMT 并列的一个 `BrokerAdapter`，不是新的 OMS、EMS、策略框架或旁路交易系统。

> 执行计划：[QMT Adapter 未完成能力开发、实施与验收计划](./17-QMT-Adapter未完成能力开发实施与验收计划.md) 将本定档尚未实施的 P0–P4 拆为代码边界、集中验收门禁和逐笔穿透时间合同；该计划统一使用命令/回调 `C0`–`C13`、恢复 `R0`–`R8` 与故障传播 `H0`–`H4`。

## 1. 结论与不可变边界

QMT 生产接入的目标是一个与 `EmtBrokerAdapter` 同等级的 `QmtBrokerAdapter`：它实现同一 `BrokerAdapter` 契约，接收同一 `ExecutionInstruction`，通过同一 `BrokerEventObserver` 回报订单、成交、错误、查询事实和恢复证据。

```text
策略 / 人工操作
        │
        ▼
TradeCore OMS ──► EMS ──► BrokerAdapter
                              ├── EmtBrokerAdapter  ──► EMT native session
                              └── QmtBrokerAdapter  ──► Windows QMT Edge ──► QMT
```

2026-09-30 已将同级边界加入自动检查：`TCB104` 禁止通用 OMS/EMS/Consumer/Adapter 合同依赖具体 venue 实现，`TCB105` 禁止 EMT/QMT 等同级 Adapter 相互依赖。QMT 的 `bridge_registry_declaration()` 与 EMT 通过同一个 `VenueAdapterRegistry` 装配；mTLS、事件 ACK 与 Windows Edge 只属于 QMT Adapter 的内部连接实现。连接开发与测试增量见执行计划 §1.5，生产运行装配仍待完成。

因此：

- OMS 继续唯一拥有 `oms_order_id`、订单状态机、资金/持仓保留、策略幂等和恢复编排；QMT 不保存第二份 OMS。
- EMS 继续唯一拥有账户路由、执行准入、保护、限频和 kill switch；Windows Edge 不做风险放行或策略决策。
- QMT SDK、Big QMT 内嵌 Python、登录态和 QMT 本机密钥只存在 Windows；Linux 不导入 `xtquant`、不保存 QMT 凭据，也不调用 QMT SDK。
- 不修改现有 `qmt_win_gateway` 的仅回环监听约束，不将其 `Gateway :9550` 或本机 bearer token 直接暴露到网线；但它不再位于生产高性能路径。
- 一个 QMT 账户对应一套隔离的 Windows `QmtTradeCoreEdge + QMT Native Engine`，以及一个 TradeCore `adapter_instance_id / adapter_client_scope`。不同账户不共享 QMT 模型实例、共享内存段、SQLite、连接、回调游标或 native caller。旧 `Helper + Gateway + Coordinator` 是独立遗留系统的历史实现，不属于 TradeCore `QmtBrokerAdapter`，不能与其使用同一账户并共同调用 QMT 下单函数。

## 2. 当前事实、官方约束与适配选择

现有 `D:\xuanling\全自动\qmt_win_gateway` 的链路如下：

```text
外置策略 → Coordinator :9560 → Gateway :9550 → 原子文件队列 → Big QMT Helper
```

它已实现单账户 primary consumer、SQLite WAL 幂等、副作用状态、原子文件 IPC、可靠 `DELIVERY_ACK`、快照 generation/hash、订单/成交去重和回调重投；Gateway 严格仅监听 `127.0.0.1`。但 Helper 使用 25 ms command timer、每 tick 最多 4 条和 15 ms budget。这一轮询/文件/多 SQLite 转发是历史网关的工程选择，**不是 QMT 官方内置 Python 对立即下单的要求**。

QMT 官方内置 Python 文档给出的、必须遵守的事实如下：

| 官方事实 | 对本方案的约束 |
| --- | --- |
| 内置 Python 运行环境按 Python 3.6 规范提供 | Native Engine 必须保持 Python 3.6 兼容；不依赖现代 CPython-only 语法、二进制扩展或 Linux 运行时。 |
| `passorder(..., quickTrade=2, ...)` 会立即发出委托并异步返回，不等待委托回报 | 生产下单使用 `quickTrade=2`；`passorder` 返回只能证明 QMT 本地调用返回，不能冒充柜台受理或最终订单状态。 |
| `run_time`、行情回调、交易回调、`after_init` 等非逐 K 线场景下的下单需传 `2`，避免信号被 K 线机制丢弃 | Native Engine 只能在 QMT 官方支持的回调上下文执行 `passorder`；禁止由 Edge 的网络线程或任意自建 Python 线程直接调用 QMT API。 |
| 立即下单的状态必须保存于普通全局对象，不能放在 `ContextInfo` 属性 | ring cursor、request 去重缓存、限速状态和测量计数使用模块级 `G` 对象；`ContextInfo` 只作为官方函数调用所需参数传入。 |
| 委托、成交、持仓、资金更新由客户端后台异步处理 | Native callback 只能作为原生事件事实；不能以 sleep、`passorder` 返回或本地队列清空伪造回调完整性。 |

因此，生产目标改为 **Embedded Native Engine mode**：Windows `QmtTradeCoreEdge` 保留跨主机身份认证、持久化幂等、可靠事件投递、恢复与健康；每账户在 Big QMT 内运行一个极小的 `QmtNativeEngine` 模型。Edge 将已持久化的固定大小命令帧写入本机 SPSC 共享内存命令环；Native Engine 在 QMT 官方 `run_time` 回调中 drain，并在同一 QMT 官方线程顺序调用 `passorder(..., quickTrade=2, ...)`。原生订单/成交回调再写入反向 SPSC 事件环，由 Edge 持久化并发送 Linux。

```text
Linux QmtBrokerAdapter
        │  mTLS command / event streams
        ▼
Windows QmtTradeCoreEdge ── Edge SQLite (authority / outbox)
        │  committed local SPSC command ring
        ▼
Big QMT embedded Python: QmtNativeEngine
        │  official run_time callback; one native caller
        ▼
passorder(..., quickTrade=2, ContextInfo)
        │
        └── QMT order/trade callback → SPSC event ring → Edge SQLite → Linux
```

这会移除热路径上的 Coordinator、Gateway TCP、原子文件、目录扫描、JSON 解析、Gateway SQLite effect forwarding 与 25 ms **外部** Helper timer；剩余的排队/调用时间应主要归因于 QMT 官方回调调度、QMT 内置 Python 和 QMT 原生 `passorder`。这不是“所有延时为零”的承诺：`run_time` 的最小有效周期、首次触发、抖动、每次 drain 可安全处理的条数以及 QMT/柜台限流都必须由 P0 实测，且不得用行情 tick 作为下单唤醒源。

以上是 TradeCore 唯一的 QMT Adapter 部署链路，正式名称为 **TradeCore QMT Adapter Path**。旧 Gateway 链路仅用于独立遗留系统的历史对比，既不是 TradeCore 的运行链路，也不是 QMT scope 的自动回退目标；Native Engine、ring 或 Edge 任何健康门禁失败时，正确动作是停新单并保持 `DISARMED`。

## 3. 目标模块与职责

建议新增如下生产模块；现有 `adapters/qmt/fake.py` 仅保留为无副作用契约夹具。

```text
xuanling_tradecore/adapters/qmt/
├── bridge_adapter.py          # QmtBrokerAdapter；唯一 OMS/EMS 下游入口
├── bridge_protocol.py         # Linux ↔ Windows 的版本化消息、hash、cursor
├── bridge_transport.py        # mTLS 双连接、重连、背压与心跳
├── bridge_normalizer.py       # Edge event → BrokerEvent，不泄漏 QMT 原生对象
├── bridge_recovery.py         # query/snapshot/replay evidence → BrokerRecoveryEvidence
├── route_token_qmt.py         # 已经 P0 证明后的 QMT native-field codec
├── profile.py                 # QmtAdapterProfile 的生产 factory
├── native_engine_contract.py  # Edge / embedded engine ring frame、commit word、版本与 CRC
├── edge_store.py              # 共享的 scope-local SQLite 请求/outbox 状态机
└── manifests/qmt-adapter-manifest.json

deployment/windows-qmt-edge/
├── edge_service.py            # Windows Service 入口；不含 OMS/EMS
├── edge_protocol.py           # Edge bridge 协议；不复用或泄漏 QMT 本机 token
├── native_ring.py             # Windows named mmap/SPSC ring；仅 fixed-size 二进制帧
├── native_engine_client.py    # command/result ring 的唯一 Edge 端读写者
├── edge_health.py             # QMT / Native Engine / callback / ring health
├── qmt_native_engine.py       # 部署至 Big QMT 的 Python 3.6 模型；不建网络/SQLite
└── service/                   # Windows Service、ACL、ring ACL 与证书安装清单
```

`qmt_native_engine.py` 是 QMT 模型脚本，不是 Windows Service 的子线程。其热函数仅做 frame commit/版本/CRC 检查、固定字段解码、极小本地反压检查、`passorder(..., quickTrade=2, ...)` 和结果 frame 提交；禁止在该函数执行 SQL、JSON、TLS、文件枚举、网络 I/O、日志格式化、全量查询或策略/风控计算。所有可恢复状态均先由 Edge SQLite 持久化，ring 只承担低延时的本机交接，不能成为唯一事实源。

### 3.1 Native ring ABI 与 Windows 安全边界

P0 先验证 QMT 内置 Python 是否能安全打开 Windows named mapping。仅在验证通过后，TradeCore QMT Adapter Path 使用预分配 `Local\\TradeCore.Qmt.<scope>.<generation>` named mapping；若该能力不可用，唯一允许的技术回退是**启动时一次打开**、固定大小、预触页的 file-backed mmap ring。该技术回退不改变 Adapter Path 的模块和语义，且不得退化为“每条命令创建/rename/扫描一个文件”，也不得成为网络共享文件。

每个 command/event slot 都是固定长度二进制结构，至少包含 ABI version、kind、scope digest、sequence、request/event ID digest、payload length、payload CRC、monotonic timestamps 和最后写入的 commit sequence。producer 写完所有 body 字段和 CRC 后，才以带内存屏障的对齐 sequence store 发布 commit；consumer 以相同 sequence 的 acquire read 验证后才读取 body。Python 3.6 的普通属性赋值或裸 mmap 切片不被假定具有跨进程 release/acquire 语义：P0 必须证明 `ctypes` 调用 Windows Interlocked 原语的位宽、对齐、失败行为和 x86_64 内存序；不能证明时 TradeCore QMT Adapter Path 不激活，不以“通常可用”代替。

ring DACL 只允许 Edge Windows Service SID 与运行 Big QMT 的专用 Windows identity 访问；名称仅使用 `Local\\` namespace，不使用 `Global\\`、SMB、映射盘或共享目录。每个 scope/generation 使用随机不可预测的 suffix，且名称、DACL 摘要和 mapping size 只保留脱敏 evidence。Edge/Native Engine 启动时须双向校验 ABI version、scope digest、generation 与 producer/consumer 角色；任何不匹配都不打开新单。

### 3.2 `BrokerAdapter` 一一映射

| 通用契约 | `QmtBrokerAdapter` 责任 | Windows Edge 责任 | 不表示 |
| --- | --- | --- | --- |
| `start(observer)` | 先挂接 durable observer，再建立事件流并恢复 cursor | 校验 Edge、Native Engine、ring 与 QMT health，提供 resume handshake | QMT 已可交易或账户 `READY` |
| `submit(instruction)` | 仅在 M1 `PRE_DISPATCH` 事实已提交后发送稳定 request | SQLite 先持久化指纹，再发布一个 committed native-command frame | QMT 最终受理、成交或挂单 |
| `cancel(instruction)` | 只撤已持久化 `VenueOrderIdentity` 的本 scope 订单 | 以相同的命令合同提交原生撤单 frame，禁止账户级扫单 | 已最终撤单 |
| `query(request)` | 产生相关 query handle；结果只经 observer 进入事实链 | 由 P0 证明的官方 QMT 查询上下文执行并写入完整性显式事件 | 空结果就是负确认 |
| `begin_recovery(plan)` | 启动只读恢复、保持 `DISARMED` | 续传未 ACK 事件，触发受控 native query/snapshot | 自动重报、自动撤单或 `READY` |
| `collect_recovery_evidence(handle)` | 输出 completeness-explicit `BrokerRecoveryEvidence` | 提供 snapshot/query generation、cursor、完成语义和映射摘要 | QMT 有公共回放能力 |

Windows Edge 只做协议翻译、可信身份检查、持久化幂等、回调可靠投递和健康投影。它不得接收策略逻辑、价格/金额决策、风险阈值或调用方自定义 QMT payload。

## 4. 跨主机通信与网络边界

Linux 与 Windows 通过两台主机间的专用 RJ45 链路通信。该链路减少网络跳数，但不等于身份认证或高可用。

```text
Linux host                              Windows host
──────────                              ────────────
TradeCore QmtBrokerAdapter ── mTLS ──► QmtTradeCoreEdge
      ▲                                      │
      └──────── event stream + ACK ──────────┘
                                               │ loopback only
                                               ▼
                                         Native Engine / QMT
```

必须使用两条长期 mTLS TCP 连接：

1. **command stream**：Linux → Edge 的 `SUBMIT`、`CANCEL`、`QUERY`、`RECOVERY_BEGIN`；命令按 scope 有界排队，返回 Edge 已持久化的接受或明确拒绝。
2. **event stream**：Edge → Linux 的 submit response、订单/成交/错误、快照可用、健康、callback gap 和恢复 evidence；每条有单调 `delivery_id`，Linux 在 mmap Journal 提交后 ACK。

网络部署要求：

- Windows 防火墙只允许指定 Linux 对端地址访问 Edge 的专用端口；Linux 只允许访问指定 Windows 对端地址和端口。
- Edge 使用双向 TLS，服务端/客户端证书均绑定 Edge instance、账户 scope、有效期和撤销来源；禁止以“物理直连”替代认证。
- QMT 登录态、账户配置及任何兼容 Gateway 的本机 token 仅保留于 Windows 的受限本机 secret store，绝不下发到 Linux、日志、evidence 或 Git。
- Linux 与 Windows 时间戳分别使用单调时钟。跨主机单向延迟只有在已记录时钟偏差/同步误差后才可计算；否则只报告各主机内分段和端到端墙钟。
- 网线、Windows Edge、Native Engine、ring 或 QMT health 任一失效，QMT scope 立即停止新单并保持 `DISARMED`；既有订单只能查询、恢复和受控人工清理。

## 5. 命令、幂等与回调语义

### 5.1 命令

```text
C0 Strategy signal admitted
 → C1 EMS authorization
 → C2 Linux PRE_DISPATCH mmap commit
 → C3 Bridge SUBMIT sent
 → C4 Edge SQLite request/correlation committed
 → C5 native command frame commit to the SPSC ring
 → C6 QMT official `run_time` drain begins
 → C7 immediately before `passorder(..., quickTrade=2, ...)`
 → C8 QMT `passorder` returns
```

`execution_id`、`request_id`、canonical intent hash、scope、route reference 和 native-frame sequence 在 Linux/Edge 两端均为不可变关联键。相同 request ID 与相同 hash 只返回已有结果；相同 ID 而 hash 不同是 `IDEMPOTENCY_CONFLICT`；C4 后超时、断线或 unknown state 永远不生成新 ID 重发，只能使用原键查询/对账。C5 frame 的 commit word 必须最后写入；Native Engine 只读取版本、长度、CRC、scope、sequence 与 Edge 已提交 request 相匹配的 frame。任何撕裂/CRC/序列/容量错误均为 `NATIVE_RING_GAP`，立即停新单并保留 `DISARMED`。

`C6→C8` 的唯一 QMT caller 是内置 Python 模型的 QMT 官方回调线程。Edge 网络线程、ring reader/writer、Windows Service、Linux adapter 和任何另一个 QMT 模型均不得直接调用 `passorder`，也不得用“更多 Python 线程”并发调用同一 QMT 账户。Native Engine 用普通模块级 `G` 对象保存 cursor、短期去重、测量和限速状态；不得将这些状态写到 `ContextInfo` 属性。

### 5.2 回调

```text
QMT native order / trade callback
 → C9 embedded callback begins
 → C10 native event frame commit to SPSC event ring
 → C11 Edge SQLite durable event + Linux outbox record
 → C12 Linux callback mmap commit
 → C13 BrokerEventObserver / OMS Projector
```

Edge 只有在其 SQLite durable event 成功后才可推进 native event ring 的 consumer cursor；Linux 只有在 C12 成功后才能 ACK Edge。Edge→Linux 仍是至少一次投递，因此 `event_id`、`delivery_id`、QMT order/trade identity 和 route reference 必须幂等。无法归属、重复版本冲突、cursor 跳跃、有界队列满或 callback ring 撕裂必须发布 callback-gap evidence 并 fail-close，而不是丢弃或猜测。`passorder` 返回、C8 result frame 或 ring cursor 推进均不能替代 C9 及以后的真实 QMT 原生 callback。

### 5.3 TradeCore QMT Adapter Path 的调度与吞吐边界

TradeCore QMT Adapter Path 维持一个且仅一个 QMT native caller：`QmtNativeEngine` 在 QMT 官方 `run_time` 回调中，顺序消费 Edge 产生的 command ring。它没有可切换的 Windows 下单 backend；技术实现只能在 named mapping 和启动时一次打开的 file-backed mapping 之间按 P0 结果选择，二者使用同一 ABI、同一 Edge SQLite request state、同一恢复语义和同一性能口径。

```text
TradeCore QmtBrokerAdapter
  → QmtTradeCoreEdge durable request
  → committed SPSC command ring
  → QMT official run_time drain
  → one QMT caller
  → passorder(..., quickTrade=2)
```

ring 是**传输优化而非耐久性替代物**：每一帧都有独立 `request_id`、intent hash、scope、sequence、CRC、before-call/after-return timestamps 与 native-result/unknown-state 记录；C4 的 Edge SQLite request state 是恢复时的权威起点。QMT/Edge/Windows 崩溃在 C5--C8 任意边界时，恢复按同一 request 查询/对账；没有可证明 native result 的 request 固定为 unknown，绝不把整个 batch 重放为第二次 `passorder`。

Adapter Path 可以在一次官方 timer 回调中顺序 drain 一个**有界自适应** batch：空闲时一条立即处理；存在积压时在经 P0 证明的 timer budget、`passorder` P95、ring backlog、QMT/券商 rate limit 和 callback health 上限内提高条数。这里的“批量”只减少回调进入、ring cursor 和 Python 固定开销；每一条仍调用一次 `passorder`，不允许将多个业务委托压缩成一个 QMT 委托。不得以行情 tick 触发作为吞吐捷径，防止无行情时订单永久等待或行情负载反压交易。

历史 Gateway 文件链路的理论领取上界约为 `4 / 25 ms = 160/s`，还会受到 15 ms tick budget 与文件 I/O 影响；该数值只用于说明为何不能沿用它，绝不是 TradeCore QMT Adapter Path 的容量结论。Adapter Path 的安全 TPS 上限只能以 P0/P4 的 `C6→C8` 单帧原始样本、timer overrun、callback gap、QMT 原生限流和清理闭环确定，不能通过缩小 timer 周期或增大 batch 配置宣称。

## 6. QMT 订单归属：P0 前不得冻结 production codec

当前 TradeCore fake QMT codec 假定 32-byte `order_remark`；当前 Windows QMT 网关文档则限定 `<PREFIX>_<TOKEN>` 最长 23 字符，并将其同时用于 `order_remark` 与 `qmt_user_order_id`。二者冲突，故 fake codec 绝不能提升为生产实现。

生产 `QmtAdapterProfile` 需要在 P0 后以签名 manifest 固定以下真实字段契约：

| 要验证的事实 | P0 通过条件 | 不通过时的行为 |
| --- | --- | --- |
| 原生字段容量/编码 | `order_remark`、`qmt_user_order_id` 的字符数、字节数、字符集和规范化均已实测 | 禁止激活 QMT profile |
| 完整 `BrokerCustomRefV1` | 在一个已证明可回显的原生字段中无损写入、回调、查询和恢复均可读回 | 禁止散列、截断或偷偷替换业务归属字段 |
| opaque `AdapterRouteToken` | native reference 在同一 scope 内无冲突，且本地 route directory 可恢复 | 不能自动关联/恢复 |
| 订单/成交/撤单错误 | 回显字段和 QMT stable identity 均可关联 | 标记 `UNRESOLVED`，保持 `DISARMED` |
| 外部订单 | 不能被 TradeCore tag 精确识别的订单默认 `UNATTRIBUTED` | 绝不自动撤单或投影为本系统订单 |

若 QMT 只有 23 字节且两个字段容量相同，配置的完整 `BrokerCustomRefV1` 必须本身满足 23 字节限制；不允许为了适配而哈希、删字符或截断。若 QMT 存在一个独立且足够大的可回显字段，可将完整 `BrokerCustomRefV1` 放在该字段，并将短 opaque route reference 用于 `order_remark`；这也必须由 P0 的回调、查询和恢复证据证明后才能写入 manifest。

## 7. 恢复与健康

恢复固定为：先阻断新单 → 挂接 event ingress → 续传 Edge 未 ACK delivery → 读取 QMT 完整快照/查询 → 仅以精确 tag/route/原生 ID 分类 → 生成 `BrokerRecoveryEvidence` → 保持 `RECOVERED_DISARMED`。

恢复证据至少包含：

- Edge build、Windows QMT Native Engine/QMT build、QMT 配置和生产 manifest hash；若以独立遗留系统进行历史对比，证据另行记录其 Gateway/Helper build；
- 事件 cursor、snapshot generation、snapshot SHA-256、快照完成时间和年龄；
- 订单、未完成订单、成交、资产、持仓查询的明确完成语义；
- TradeCore own、foreign reserved、unattributed、conflict、missing-route 的脱敏计数与 digest；
- callback gap、未知副作用、映射冲突、查询超时和负确认缺口。

当前 QMT 网关的 snapshot `generation/hash` 可作为重要输入，但单独一个快照文件不能证明券商的全局回放或查询完整性。无页末/完成标记、无稳定 ID、无完整回显或有 gap 时，恢复结果必须为 `RECOVERY_BLOCKED` 或 `RECOVERED_DISARMED`。

## 8. 性能口径

QMT 必须独立验收，不能借用 EMT 的 499-burst 结果。每次采样至少报告：

| 分段 | 含义 |
| --- | --- |
| C0→C2 | TradeCore 策略、OMS/EMS 与 Linux mmap 热路径 |
| C2→C4 | Linux 至 Windows Edge 传输与 Edge 幂等持久化 |
| C4→C5 | Edge request durable 后的命令 ring 发布；不含 QMT 调度 |
| C5→C6 | QMT 官方 scheduler 等待；是 Adapter Path 的可测非 native 排队 |
| C6→C8 | QMT 内置 Python frame drain、固定参数构造与 `passorder(..., quickTrade=2)` 原生调用返回 |
| C9→C11 | QMT 原生 callback 至 Edge durable event；与 C8 绝不相加冒充柜台回报 |
| C11→C12 | Edge 至 Linux callback Journal durable |
| C12→C13 | Linux projector/消费者可见性 |
| C0→C13 | 一笔端到端 callback 链路；与 native-return、首回报、最终状态分别报告 |

同时记录单笔、100、300 和 QMT 经 P0 证明的最大安全 burst 的逐笔 P50/P95/P99/P100、批次墙钟、成功/明确背压/权威 venue throttle/unknown-state 计数。每次结果必须分开报告 `C5→C6`（QMT 调度等待）和 `C6→C8`（QMT 内置 Python/native call），不得把两者合并后宣称“纯原生调用延迟”。不得承诺微秒级、沿用 EMT 的吞吐/SLA，或将历史 file mode 的 160/s 上界用于 TradeCore QMT Adapter Path。

## 9. Windows 优化优先级

Windows 侧优化以“先验证官方可调用上下文和回调语义，再建立 Edge 的可靠边界，最后将 Windows→QMT 热路径收敛为官方 native 调度与 `passorder`”为固定顺序。每项优化不得降低 Edge SQLite 的幂等/unknown-state、Linux durable ACK、单账户单 native caller 或 QMT 仅本机凭据边界。

| 优先级 | 工作项 | 预期收益 | 前置/验收 | 当前决定 |
| --- | --- | --- | --- | --- |
| P0 | QMT Native Engine capability probe：Python 3.6、`run_time` 的有效最小周期/首次触发/抖动、`quickTrade=2`、单 caller、order/trade callback、查询/撤单、QMT 限流与重连 | 确定可由官方支持的 native execution contract，不把猜测写进代码 | 仿真账户、脱敏原始样本、无 ContextInfo 状态回退；任何不完整项 `CAPABILITY_UNAVAILABLE` | **必须先完成** |
| P1 | `QmtTradeCoreEdge`：mTLS、Edge SQLite、双流 cursor/ACK、Windows Service、Windows ACL | 保留跨主机可靠幂等与恢复，且不使其进入 Native Engine 热函数 | 断线/重投、ACL、secret-leak、unknown-state、Linux/Edge 重启回归 | **必须实施** |
| P1 | TradeCore QMT Adapter Path：固定大小 SPSC command/event ring、QMT Python 3.6 Native Engine、`run_time` drain、`quickTrade=2` 与 native callback ingress | 删除 Coordinator、Gateway、文件、目录扫描与 JSON 转发，令 C5 后主要等待落在 QMT 调度/原生调用 | P0 通过；单 caller、commit-word、CRC、ring wrap、崩溃边界、callback gap 与清理闭环 | **必须实施** |
| P1 | 全链路分段观测：C4/C5/C6/C7/C8/C9/C10/C11/C12/C13 | 把非 native scheduler 等待同 `passorder` 调用、callback、Edge I/O 清晰分离 | 仅脱敏时间戳/correlation digest；禁止记录订单载荷、QMT token 或 secret | **必须实施** |
| P2 | 自适应 native drain：低负载 1 条；积压时按 P0 的 `passorder` P95、timer overrun、QMT rate limit 与 callback health 选择有界 batch | 降低 burst 的每帧固定调度开销，同时不牺牲单笔低延时 | 1/100/300/max-safe burst 与故障回归；逐条 unknown-state/清理核对 | **条件实施** |
| P2 | Windows 运行调优：Native Engine/QMT 进程亲和性、Edge 独立核、网卡 IRQ/QoS、低延时盘上的 Edge SQLite | 降低 C2→C5 与 C5→C6 抖动，不能假定提高 QMT 原生吞吐 | 固定环境 A/B，按 P95/P99/P100 和 callback gap 决策 | **按实测选择** |
| P3 | Adapter Path 的故障隔离、禁用与受控恢复 | QMT Native Engine / ring / Edge 失败时停新单、保留 unknown-state 并由权威查询恢复；不把旧 Gateway 重新引入本适配器 | Linux/Edge/QMT 分别重启，快照、cursor、unknown-state 与 route 分类完整对账 | **必须实施** |

以下动作在当前阶段明确禁止：把 Gateway 改为非回环监听；把 QMT token 传到 Linux；将 Edge SQLite `synchronous` 降为 `OFF`；让 ring 成为唯一事实源；从 Edge 网络线程、辅助线程或多个模型并发调用同一 QMT session；借用行情 tick 作为订单唤醒源；仅凭物理直连宣称网络可信或高可用。

### 9.1 Adapter Path 的 Edge durable delivery 固定策略

TradeCore QMT Adapter Path 不启动 Coordinator，也不让 Edge 成为 Gateway TCP primary client。Edge 是唯一 Linux bridge peer、唯一 Edge SQLite writer、唯一 command-ring producer、唯一 event-ring consumer；QMT Native Engine 是唯一 command-ring consumer、唯一 event-ring producer、唯一 QMT native caller。SPSC 角色不得交换或共享。

```text
Linux SUBMIT
  → Edge SQLite request durable commit
  → command frame body / CRC
  → write commit word last
  → QMT run_time drain / passorder

QMT native callback
  → event frame body / CRC
  → write commit word last
  → Edge SQLite durable event + Linux outbox
  → Linux callback mmap durable commit
  → Linux ACK advances Edge outbox cursor
```

命令 ring 已满时，Edge 在 C4 前返回确定性的 `EDGE_BACKPRESSURE`；不得阻塞 Windows 网络接收线程、不得把新命令静默落到无界内存队列。event ring 接近满时，Edge health 先置为 not-ready 并停新单；实际满、序号跳跃、CRC/版本错误或 consumer 落后超过保留窗必须生成 callback-gap evidence。Linux ACK 缺失只会导致 Edge 从 SQLite outbox 重投，不会将同一命令再次写入 native ring。

Adapter Path 取消 `POLL_EVENTS` 空轮询、Coordinator 写锁竞争、Gateway effect forwarding 和文件命令扫描。若 P1 指标显示 `C5→C6` 仍为主瓶颈，只能在 P0 证明的 `run_time` 调度范围内调节周期/批量和 Windows 调度；若 `C6→C8` 主导，则分析 QMT 原生 `passorder`/账户限流。两种情况都不得用多 caller 并发调用 QMT 规避。

## 10. 分包实施与统一验收

| 包 | 代码范围 | 本包完成定义 | 统一验收时机 |
| --- | --- | --- | --- |
| P0 | 实际 QMT 字段/回调/查询/限流/线程语义，Python 3.6、`quickTrade=2`、`run_time` 周期/抖动、native callback 与 ring ABI 探针；脱敏 evidence schema | 生产 manifest 和 Native Engine 所需事实均有真实证据，或明确 `CAPABILITY_UNAVAILABLE` | P0 全部探针完成后一次验收；不下结论为 `READY` |
| P1 | `QmtTradeCoreEdge`、mTLS、Edge SQLite、双流 cursor/ACK、Windows Service、ring ACL | 无 QMT SDK/token 越过 Windows；Edge 持久化与 Linux replay 合同独立于 QMT native loop | P1 功能完成后做 protocol、断线、重投、ACL、secret-leak、ring backpressure 回归 |
| P2 | Big QMT Python 3.6 `QmtNativeEngine`、SPSC command/event rings、`QmtBrokerAdapter`、生产 profile/manifest、mapping、normalizer | `BrokerAdapter` 五项方法均接通，且唯一 native caller 只在官方上下文以 `quickTrade=2` 调用；fake 仍只读 | P2 后统一做 submit/cancel/query/callback/recovery、ring crash-boundary 与 callback gap contract 验收 |
| P3 | 故障与恢复 | Linux/Edge/QMT Native Engine 分别重启，零重复下单、零跨 scope 影响；unknown-state、callback ACK 与 route 分类保持一致 | P3 后一次真实仿真恢复演练 |
| P4 | QMT 性能与容量 | 固定环境、脱敏原始时间对、清理闭环、明确 QMT timer/native/venue 限流边界；历史 Gateway 链路只作为独立基线对比 | P0–P3 都通过后，统一性能压测 |

P0 之前，QMT 生产 profile 继续为 `CAPABILITY_UNAVAILABLE`，fake adapter 继续拒绝 `submit/cancel`。P0–P4 任意一个未通过都不影响 EMT 已验收路径，但会阻止 QMT scope 离开 `DISARMED`。

## 11. P0 首次执行清单

P0 只在 Windows QMT 仿真环境、单一受控账户和脱敏 evidence 下执行。第一轮至少验证：

1. Big QMT build、内置 Python 3.6 兼容性、模型生命周期、`after_init` / `run_time` 的实际调用上下文；`run_time` 以历史 start time 注册时的首次触发、最小可接受 `nMilliSecond` 周期、P50/P95/P99/P100 抖动、overrun 和停模型行为；
2. `passorder(..., quickTrade=2, ...)` 在经官方支持的 timer、行情 callback、交易 callback 上的立即发出/异步返回事实；禁止把 `passorder` 返回解释为柜台/订单回报；
3. 普通全局 `G` 对象与 `ContextInfo` 属性在快速交易模式下的状态生命周期差异；任何 ContextInfo rollback/重置风险均禁止进入 Native Engine；
4. `order_remark`、`qmt_user_order_id` 的真实容量、编码、无损回显、空值与非法值拒绝行为；
5. 订单受理、订单回调、成交回调、撤单错误、订单查询、成交查询和 snapshot 中的 route/identity 回显；
6. QMT `order_id`、`order_sysid`、trade identity、撤单 identity 的稳定性与可关联性；
7. command/event ring 的 ABI、commit-last、CRC、wrap、producer/consumer crash、满载 backpressure 与 Edge SQLite 对账；
8. 完整快照、订单/成交/未完成订单、资产、持仓查询的完成语义；
9. QMT 单调用线程约束、并发/限流、disconnect/reconnect、native callback 线程行为和 callback-gap 检测；
10. 外部订单与缺失/冲突 tag 的 `UNATTRIBUTED` / fail-closed 分类。

P0 的输出只允许包含版本/hash、字段长度、布尔结论、计数、错误类别、时间段摘要和脱敏 correlation digest；不得包含账户、证券、价格、订单号、IP、密码、证书私钥或 QMT token。

## 12. 官方依据与设计解释

- [QMT 内置 Python 快速开始](https://dict.thinktrader.net/innerApi/start_now.html)：内置 Python 3.6、逐 K 线与 `quickTrade=2` 的区别、立即下单时不得保存 `ContextInfo` 状态。
- [QMT 常见问题](https://dict.thinktrader.net/innerApi/question_answer.html)：`quickTrade=2` 的 `passorder` 立即发委托后异步返回，以及委托/成交/资金持仓由客户端后台更新的边界。
- [QMT 系统函数](https://dict.thinktrader.net/innerApi/system_function.html)：`run_time` 支持 `nMilliSecond` 周期，首次触发和实际精度须运行时验证。
- [QMT 交易函数](https://dict.thinktrader.net/innerApi/trading_function.html)：`passorder` 的统一下单参数、策略名与 `userOrderId` 的本机策略区分语义。

上述文档证明“在官方 QMT 运行上下文中以 `quickTrade=2` 发起后，不被逐 K 线机制延后”，**不**证明任意 timer 精度、`passorder` 的柜台受理、回调完整性、跨进程 mmap 可用性、同账户并发上限或恢复/回放语义。因此这些项目均留在 P0/P3/P4 的实测与 fail-closed 门禁中。

## 13. 代码实现定档

本章把架构拆为可直接实现的代码边界。当前 production-shaped Adapter 的 profile factory 默认关闭 mutation；P0 capability activation gate 尚未实现，因此不得用它创建 QMT session、打开 native ring 或执行 `submit/cancel`。完成后的所有运行时入口必须先检查已签名 profile、scope ownership、P0 capability manifest、Edge identity、证书有效性与 `DISARMED` control state。

### 13.0 当前实现状态（2026-09-16）

已实现并通过本地内部验收的范围：Linux `QmtBrokerAdapter`、typed Edge port、submit/cancel/query/recovery 的 profile/scope fencing、Edge delivery ACK、重复事件去重与冲突 fail-closed、`QmtBridgeEventNormalizer`、bridge lifecycle guard、scope-local Edge SQLite 请求序号/幂等/unknown-effect/outbox 状态机、签名 `QmtCapabilityManifestV1` / scope-bound activation gate，以及固定长度 `QmtNativeFrameV1` / ring header / CRC / commit-sequence ABI。相关测试不导入 vendor SDK、不创建网络连接、不打开 Windows shared memory，也不会发出委托。

尚未实现或尚未验证的范围：mTLS socket transport、Windows Edge service composition、SPSC mapping、QMT Python 3.6 Native Engine、`passorder(..., quickTrade=2)`、QMT 原生 callback/query、P0 capability evidence、真实崩溃恢复与性能验收。因此 production QMT profile 仍固定为 `CAPABILITY_UNAVAILABLE`，所有 scope 继续保持 `DISARMED`。

### 13.1 代码目录与依赖方向

```text
xuanling_tradecore/adapters/qmt/
├── bridge_adapter.py          # Linux QmtBrokerAdapter；实现公共 BrokerAdapter
├── bridge_protocol.py         # 仅 Linux/Edge 间的 canonical command/event envelope
├── bridge_transport.py        # mTLS 双 stream、cursor、ACK、断线与背压
├── bridge_normalizer.py       # Edge event → BrokerEvent / CallbackGapEvidence
├── bridge_recovery.py         # query/scan/replay evidence → BrokerRecoveryEvidence
├── edge_store.py              # scope-local SQLite 请求/outbox 状态机
├── native_engine_contract.py  # 与 Windows/QMT 共享的固定帧 ABI、CRC 与枚举
├── route_token.py             # P0 证明后的 QMT route/ref codec
├── mapping.py                 # 现有 QMT mapping registry；扩展 production manifest loader
├── profile.py                 # QmtAdapterProfile production factory
└── manifests/
    └── qmt-adapter-manifest.json

deployment/windows-qmt-edge/
├── edge_service.py            # Windows Service composition root
├── edge_protocol.py           # Linux bridge server 与证书绑定
├── native_ring.py             # command/event SPSC mapping 的 Edge 端实现
├── native_engine_client.py    # publish command、drain event、health、cursor
├── edge_health.py             # Edge / QMT / ring health projection
├── qmt_native_engine.py       # Big QMT 内置 Python 3.6 模型脚本
└── service/                   # Windows Service、DACL、证书和运行目录配置
```

依赖只能单向流动：`OMS/EMS → QmtBrokerAdapter → bridge protocol → Edge → native ring → QmtNativeEngine → QMT`。反向事实只能经 `QMT callback → native ring → Edge durable event → bridge event → QmtBrokerAdapter → BrokerEventObserver` 回到 OMS。QMT 内置脚本不得 import `xuanling_tradecore`、不得访问 Edge SQLite、不得建立 TLS 连接，也不得包含策略、风控或 OMS 状态机。

### 13.2 Linux `QmtBrokerAdapter` 的实现

`bridge_adapter.py` 必须直接实现现有 [`BrokerAdapter`](../../xuanling_tradecore/adapters/base.py) 契约，保留 `FakeQmtBrokerAdapter` 用于无 SDK 的契约夹具。生产类不新增另一套 `submit` 语义：返回 `SubmitAccepted` 只代表 Edge 已以稳定 request key 接收，订单状态只能由后续 `BrokerEvent` 推进。

```python
class QmtBrokerAdapter(BrokerAdapter):
    def start(self, observer: BrokerEventObserver) -> None: ...
    def stop(self) -> None: ...
    def submit(self, instruction: ExecutionInstruction) -> SubmitAccepted: ...
    def cancel(self, instruction: CancelExecutionInstruction) -> CancelAccepted: ...
    def query(self, request: BrokerQuery) -> QueryHandle: ...
    def begin_recovery(
        self, plan: RecoveryPlan, observer: BrokerEventObserver
    ) -> RecoveryHandle: ...
    def collect_recovery_evidence(
        self, handle: RecoveryHandle
    ) -> BrokerRecoveryEvidence: ...
```

实现顺序固定如下：

1. `start` 先保存 observer，再连接 event stream、提交 resume cursor，最后连接 command stream；任何 replay event 都先经 Linux callback mmap Journal。
2. `submit` 验证 venue/account/adapter instance/scope，读取已经由 EMS 提交的 `PRE_DISPATCH` 关联；构造 immutable `SUBMIT` envelope，以 `execution_id` 为 request key。超时、重复调用和连接重建只重发同一个 envelope。
3. `cancel` 只接受已证明属于本 adapter instance 的 `VenueOrderIdentity`；不允许由 symbol、账户或 QMT 原生订单号的裸字符串构造撤单。
4. `query` 与 `begin_recovery` 只写入 read-only command；接收端必须以 `QUERY_COMPLETE`、分页/末页或官方 P0 证明的终态事件结束，静默和空数组不是完成证据。
5. event reader 先由 `bridge_normalizer.py` 校验 source identity、`delivery_id` 单调性、payload hash 和 route ownership；成功后才调用 observer。冲突、跳号、越 scope、未知 event kind 或 ring gap 一律调用 `on_callback_gap`。

`bridge_adapter.py` 不能等待 QMT native return、不能 sleep 等待回报、不能在 Linux 侧用额外线程并发伪造 QMT native lane。其 command stream 仅负责跨主机有界投递；QMT 单 caller 约束由 Windows Native Engine 保证。

### 13.3 Linux ↔ Edge 协议与连接实现

`bridge_protocol.py` 定义稳定的 envelope，而不是把 Python dataclass、QMT 对象或自由格式字典跨机传输。所有 payload 在发送前 canonical encode 并计算 hash；网络帧使用长度前缀、magic、schema revision、kind、request/event ID、scope digest、payload length、payload hash 和 payload。实现可采用 `struct` 固定 header 加确定性的 UTF-8/binary payload；QMT 内置 Python不参与这一协议编解码。

| command kind | 最小字段 | Edge 成功含义 |
| --- | --- | --- |
| `SUBMIT` | request/execution ID、scope、intent hash、canonical order fields、route reference | `EDGE_ACCEPTED`：请求幂等记录已写入 Edge store；不代表 QMT 或柜台受理。 |
| `CANCEL` | request ID、proven venue identity、scope、cancel reason digest | `EDGE_ACCEPTED`：已绑定精确目标；不代表撤单完成。 |
| `QUERY` | request ID、query kind、page/cursor、read-only scope | `QUERY_ACCEPTED`：已登记，结果经 event stream 返回。 |
| `RECOVERY_BEGIN` | recovery run ID、scope、query plan digest、last durable cursor | `RECOVERY_ACCEPTED`：开始只读恢复，仍保持 `DISARMED`。 |
| `ACK` | `delivery_id`、event hash、Linux callback journal digest | 仅推进 Edge outbox；绝不影响 QMT command ring。 |

`bridge_transport.py` 使用两个独立 mTLS TCP 连接：command stream 仅 Linux→Edge；event stream 仅 Edge→Linux。双方证书必须绑定 adapter instance、scope 与有效期；连接重建用 server/peer cursor 协商，不使用“重新登录后从头推送”代替。每 scope 使用一个有界 command writer 与一个 event reader；队列满时返回 `EDGE_BACKPRESSURE`，不创建无界 `deque`、线程池或重试风暴。

### 13.4 Edge 持久化模型与事务边界

`edge_store.py` 每一个 adapter scope 一个 SQLite 文件、一个 writer actor 和一个受限运行目录。数据库使用 WAL，禁止 `synchronous=OFF`；在当前“进程崩溃而非主机掉电恢复”范围内，`NORMAL`/`FULL` 的最终选择由 P1 固定环境延迟与恢复回归共同决定并写入 profile，不能由热路径代码擅自改写。

| 表 | 主键/唯一约束 | 用途 |
| --- | --- | --- |
| `requests` | `request_id`；`request_id + intent_hash` | 幂等提交/撤单/查询、Edge 生命周期、deadline、ring sequence、unknown reason。相同 ID 不同 hash 拒绝。 |
| `native_identity_map` | `(scope, qmt_order_id)`；route reference | 仅保存经 native callback/query 证明的双向 QMT identity 关联。 |
| `events` | `event_id`；`source_kind + source_sequence + payload_hash` | 原生 callback、call-started、call-returned、query row/complete、health/gap 的不可变事实。 |
| `outbox` | 单调 `delivery_id` | 发送 Linux 前的可靠事件；只有收到匹配 ACK 才标记完成。 |
| `ring_cursor` | `ring_name + direction` | producer/consumer sequence、generation、最近 heartbeat、gap state。 |
| `recovery_runs` | `recovery_run_id` | 查询计划、completion 摘要、证据 hash 与 final gate。 |
| `health` | `scope` | Edge、ring、Native Engine、QMT connection、outbox backlog 和 stop-new-order 原因。 |

提交路径严格按以下伪代码执行：

```python
def accept_submit(envelope):
    require_scope_and_certificate(envelope)
    if command_ring.is_near_full():
        raise EdgeBackpressure()

    with store.transaction():
        row = store.get_request(envelope.request_id)
        if row and row.intent_hash != envelope.intent_hash:
            raise IdempotencyConflict()
        if row:
            return row.acceptance
        seq = store.allocate_command_sequence()
        store.insert_request(envelope, sequence=seq, state="EDGE_ACCEPTED")

    # C5：只发布已持久化 request；frame commit word 最后写。
    native_ring.publish(command_frame(envelope, sequence=seq))
    store.record_publish_attempt(envelope.request_id, seq)
    return EdgeAccepted(envelope.request_id, seq)
```

若进程在 C4/C5/C8 任一边界退出，恢复程序不会用新 ID 或新 `passorder` 重新提交。它读取 `requests`、ring sequence、native result event 和权威 QMT 查询：无法证明“尚未 native call”的项目固定为 `UNKNOWN_EFFECT`，停止新单并进入人工/受控 query 流程。

### 13.5 SPSC ring 的代码契约

`native_engine_contract.py` 是 Linux/Edge/Native Engine 的唯一 ABI 定义来源；发布时将其中的常量、字段 offset、slot size 与 hash 固化到 QMT adapter manifest。ring header 和 slot 都使用 `struct.Struct`，不得通过 pickle、JSON 或 Python object 共享内存。

```text
ring header
  magic | ABI revision | scope digest | generation | slot size | capacity
  producer sequence | consumer sequence | producer heartbeat | consumer heartbeat

command slot（固定长度、cache-line 对齐）
  sequence | kind | flags | payload length | request digest | intent hash
  C4/C5/C7/C8 monotonic timestamps | fixed QMT order payload | CRC32 | commit sequence

event slot（固定长度、cache-line 对齐）
  sequence | event kind | payload length | event digest | native identity digest
  C9/C10 timestamps | fixed callback/query payload | CRC32 | commit sequence
```

producer 顺序：检查 slot sequence → 写完整 body → 写 CRC → Interlocked release-store `commit_sequence`。consumer 顺序：Interlocked acquire-read `commit_sequence` → 检查预期 sequence / length / CRC / scope → 复制 body 到进程私有对象 → 推进 local cursor。任何不匹配不尝试“猜测下一个 slot”；Edge 写入 `CALLBACK_GAP`/`NATIVE_RING_GAP` 后停止新单。P0 必须实测 QMT Python 3.6 中 `ctypes` + Windows Interlocked 的可用性；未通过时不运行 production Adapter Path。

### 13.6 QMT 内置 Python `QmtNativeEngine` 的实现

`qmt_native_engine.py` 只兼容 QMT 的 Python 3.6。所有长期状态都放在普通模块全局对象 `G` 中；`ContextInfo` 仅用于 `run_time` 注册与调用 `passorder`。初始化时打开已经存在的 ring、验证 scope/generation/ABI/DACL 摘要、写 heartbeat，并注册一个由 P0 确认的 `nMilliSecond` `run_time` callback。

```python
# QMT Python 3.6 compatible pseudo-code; no network, SQL or filesystem scan.
class G(object):
    command_ring = None
    event_ring = None
    draining = False
    halted = True
    max_drain = 1          # P0/P2 manifest value, never hard-coded in strategy logic

def init(C):
    G.command_ring, G.event_ring = open_and_validate_rings()
    G.halted = False
    C.run_time("tradecore_qmt_drain", configured_period(), historical_start_time())

def tradecore_qmt_drain(C):
    if G.halted or G.draining:
        return
    G.draining = True
    try:
        for frame in drain_at_most(G.max_drain):
            validate_committed_command(frame)
            emit_call_started(frame)       # C7 evidence before native call
            passorder(*decode_qmt_order_call(frame, C))  # encoded quickTrade is always 2
            emit_call_returned(frame)      # C8; not an order callback
    except RingIntegrityError as error:
        emit_native_gap(error)
        G.halted = True
    finally:
        G.draining = False
```

实际订单/成交/撤单/查询 callback 的函数名、参数和完整终态以 P0 结果生成一个很薄的 QMT shim：它只将 native fields 规范化为 fixed event payload、记录 C9/C10、写入 event ring。不得在 callback 中查询账户、拼接 JSON、写日志文件、访问网络或调用 Linux 服务。event ring 无空间、native identity 不可关联、callback 参数异常或 `passorder` 抛异常时，只写最小 fault event 并 `halted=True`；Edge 从 health event 进入 stop-new-order，不允许忽略错误继续接单。

### 13.7 正常化、身份映射与恢复实现

`bridge_normalizer.py` 只接收已经由 Edge durable 的 event。它将 QMT 的订单/成交/撤单错误/查询行映射为现有 domain `BrokerEvent`，并保留 `event_id`、`source sequence`、adapter instance、route reference、稳定 native identity 与 payload hash。它绝不将 QMT 原生对象、账号凭据或自由文本错误透传给 OMS。

`route_token.py` 只能依据 P0 证明后的字段容量、编码和回显行为生成 `BrokerCustomRefV1` / `AdapterRouteToken`。字段截断、散列替代、不可逆编码、同 scope 冲突、foreign order 或 callback 无 route 时分别产生 `ROUTE_TOKEN_INVALID`、`IDEMPOTENCY_CONFLICT`、`UNATTRIBUTED` 或 callback-gap；不能为了继续下单而降级关联规则。

`bridge_recovery.py` 的顺序固定为：attach event stream → replay Edge outbox → native query/scan → 验证 query completion → 按 exact route/native identity 分类 → 生成 `BrokerRecoveryEvidence`。恢复仅输出 `RECOVERED_DISARMED` 或 `RECOVERY_BLOCKED`；不会自动重报、自动撤单或令 scope `READY`。

### 13.8 实施顺序、测试文件与完成门禁

| 包 | 新增测试/演练 | 完成门禁 |
| --- | --- | --- |
| P0 | `test_qmt_native_probe_*`、Python 3.6 mapping/Interlocked/ring ABI probe、`quickTrade=2`/timer/callback/query 脱敏实测 | 每项 capability 有原始时间对或明确 `CAPABILITY_UNAVAILABLE`；未证明不进入 P1。 |
| P1 | `test_qmt_bridge_protocol_*`、`test_qmt_edge_store_*`、`test_qmt_ring_*`、mTLS/certificate/ACL/backpressure 单元与集成测试 | request 幂等、outbox 重投、ring 撕裂/满载、Edge 重启均 fail-closed；无 QMT secret 出现在日志/仓库。 |
| P2 | `test_qmt_adapter_contract_*`、Native Engine 仿真 ring fixture、submit/cancel/query/callback normalizer 回归 | 所有 `BrokerAdapter` 方法接通；QMT native caller 仍严格为一个。 |
| P3 | Edge/Native Engine/Linux 分别强杀、C4/C5/C7/C8 crash boundary、callback gap、recovery classification | 零重复 `passorder`、零跨 scope 投影；unknown effect 不被自动消除。 |
| P4 | 热态单笔、100/300/max-safe burst、完整 C0→C13、清理闭环、限流和 callback 完整性 | 分开报告 C5→C6 与 C6→C8；只有所有 gap/unknown/cleanup 门禁通过才发布性能结论。 |

代码实现期间不得修改 EMT 的 C0--C7 运行路径。任何 QMT Adapter Path 代码或 profile 未完成时，现有 fake adapter 继续拒绝真实 `submit/cancel`，QMT scope 继续为 `CAPABILITY_UNAVAILABLE` / `DISARMED`。
