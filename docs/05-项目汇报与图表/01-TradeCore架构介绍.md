# TradeCore：产品架构与落地路线

> 用途：项目汇报架构概览；历史 P0/M 阶段文字保留用于解释演进，不作为当前验收状态。
> 当前权威结论（2026-09-11，`dev`）：EMT-only 的 F0、F1、F2、F3、F4、F6 已完成；F4 的 OMS W7 已通过。当前仍为 `DISARMED`，R1 同账户多系统权威恢复、真实 PaperBroker/QMT、SDK 全矩阵及跨主机 HA 不在已完成范围。
> 当前状态与证据以[当前未实施功能与优化收口路线](../01-架构与定档/10-当前未实施功能与优化收口路线.md)及[BenchmarkStandardV1 复验报告](../04-测试验收与性能/04-OMS性能与容量/05-BenchmarkStandardV1复验报告-2026-09-11.md)为准。
> 部署边界：单台指定 Linux x86_64 主机；不支持 Windows、跨主机 HA、active-active 或自动故障接管。

## 1. 项目定位

`emt_gateway` 是供内部策略和人工控制面调用的可插拔交易执行模块，而不是 EMT SDK 的薄封装。完整产品的固定链路为：

```text
消费者策略 / 受控人工操作
        ↓
ConsumerGateway（身份、账户/策略 binding、可靠回执）
        ↓
OMS（订单事实、幂等、冻结、状态、追溯）
        ↓
EMS（账户路由、保护、执行 lane、会话、Kill Switch）
        ↓
BrokerAdapter（EMT / QMT / PaperBroker）
        ↓
券商柜台 / 本地确定性撮合
```

消费者只能登录、查看自己获授权的账户名称、提交绑定策略的信号，并通过可靠事件追踪结果；任何策略、人工界面都不能直接取得柜台凭据、SDK session 或 Journal Writer。

通用消费者契约只使用 `VenueOrderIdentity`（`venue_id`、`adapter_instance_id`、`client_order_ref`、可选 `venue_order_id`）；EMT 的 `order_client_id / order_emt_id` 仅是 EMT profile 扩展，不能成为 QMT 或未来柜台消费者接口的必填字段。

项目不包含行情订阅、行情缓存、策略计算或跨主机高可用。策略不得绕过 OMS/EMS 直接调用 EMT、QMT 或任何其他柜台 SDK。

## 2. 当前 EMT-only 与产品扩展的边界

| 层次 | 当前 EMT-only 已完成范围 | 后续产品扩展 |
| --- | --- | --- |
| Gateway / EMT Adapter | 分片 EMT session、报单/撤单窄适配、mmap Journal、callback ingress、查询关联及受控模拟盘验收。 | 增加经独立定档的柜台或市场 profile，不改变 EMT 热路径。 |
| OMS | Journal-first 订单事实、可重建投影、immutable snapshot、watermark/health、replay/pause/stale 演练。 | 完整 R1 权威恢复证书、更多市场的规则/币种扩展。 |
| EMS | 路由、执行 lane、限频/保护、Kill Switch、会话监督、本机所有权栅栏与 scope 分片。 | 真实 PaperBroker/QMT、更多账户和更高层执行算法。 |
| Adapter | `BrokerAdapter` 合同与 EMT-only profile 已验收。 | PaperBroker/QMT 的官方 SDK、凭据、能力矩阵及独立恢复验收。 |
| 消费者数据面 | receipt、事件 cursor、绑定及审计查询契约已落地。 | 跨产品消费者与多市场授权的独立产品化。 |
| 持久化 | mmap Journal、异步 seal/read model、PostgreSQL 查询模型与 projector 合同已落地。 | 跨主机复制、掉电 RPO/RTO 与独立故障域。 |

因此可以表述为“EMT-only OMS/EMS 主链路已验收”，但不能把模拟盘证据表述为生产 SLA、`READY`、自动恢复或完整多 Adapter 产品。

## 3. 目标架构与职责边界

可编辑总图见 [03-Mermaid 图代码：图 1](./03-Mermaid图代码.md#图-1目标产品总体架构)。

| 模块 | 唯一职责 | 明确禁止 |
| --- | --- | --- |
| ConsumerGateway | 会话、消费者/账户/策略 binding、回执、事件 cursor、审计查询。 | 直接访问柜台 session、凭据或写 Journal。 |
| OMS | `oms_order_id`、状态机、幂等、归属、冻结、回放与对账决策。 | 用 PG 投影授权副作用，或把柜台同步返回当终态。 |
| EMS | Adapter 路由、执行 lane、价格/频率保护、Kill Switch、会话和账户栅栏。 | 保存策略订单簿，或绕过副作用前 Journal。 |
| BrokerAdapter | 柜台协议映射、回调规范化、只读查询和恢复证据。 | 向领域层泄漏 EMT/QMT SDK 类型或枚举。 |
| PersistentHotJournal | 热路径的同步、可恢复业务事实。 | 用普通易失内存、PG、缓存或日志替代 commit。 |
| SSD / PostgreSQL | SSD 为已 seal 的异步耐久副本；PG 为异步、可重建 read model。 | 阻塞 C0–C7，或决定恢复/报撤权限。 |

领域核心只见版本化的 `BrokerEvent` 和：

```text
VenueOrderIdentity(
  venue_id,
  adapter_instance_id,
  adapter_client_scope,
  client_order_ref,
  venue_order_id,
  venue_cancel_id,
)
```

EMT 的 `client_id / order_client_id / order_emt_id / order_cancel_emt_id` 只是 EMT profile 的字段映射；QMT 或其他柜台必须独立证明自己的关联字段、回放与查询能力。

## 4. 消费者视角：下单、回执与可追溯性

策略调用 `SubmitSignal` 后，OMS 先以 `(strategy_id, strategy_order_id)` 与 canonical `intent_hash` 做持久幂等。相同意图返回原订单；同一键但意图不同返回 `IDEMPOTENCY_CONFLICT`，不创建冻结、不调用 Adapter。

成功接受的最小事实链为：

```text
consumer_id
→ consumer_account_id / account_display_name_snapshot / binding_version
→ strategy_id / strategy_order_id
→ oms_order_id
→ execution_id
→ venue_id / adapter_instance_id / VenueOrderIdentity
→ TradingDayEpoch
→ journal_id : journal_lsn
```

`IntentReceipt.accepted_event_id` 只在 `CALL_STARTED` 已耐久后返回。订单、成交、撤单、账户状态与恢复结论以 `ConsumerEvent` 按每账户 cursor 至少一次投递；消费者在自身业务状态落库后保存 cursor，并按 `consumer_event_id` 去重。cursor 绑定消费者、账户、`journal_id + stream_generation`、下一待读 LSN 与授权 epoch，并以稳定 `AccountControlRoot` 中的 HMAC key 完整性保护；它不进入 OMS/EMT 热路径。

账户可用性由三个正交维度表达：`AccountLifecycle`（`BOOTSTRAP / RECOVERING / DISARMED / READY`）、`ReconciliationStatus` 与一组 `AdmissionGate`。`BLOCK_NEW` 只是加入 gate 的动作；消费者从 `ACCOUNT_STATUS` 读取 lifecycle、对账状态和非敏感 gate 原因，而不是仅凭连接可用判断能否下单。`KILL_SWITCH / OPERATOR_STOP / RISK_STALE / JOURNAL_CAPACITY` 仅阻断新单；callback、Journal、session 或对账证据故障则必须先 `DISARMED + REQUIRED`，开始恢复才进入 `RECOVERING + RUNNING`。

消费者 API、事件和快照均有显式 schema 版本；不兼容客户端必须收到 `UPGRADE_REQUIRED`，不得静默跳过事件或推进 cursor。mapping、capability、retention、`AvailabilityObjective` 与 schema 生命周期策略只接受由本地 `BundleTrustPolicy` 信任根离线验签的 bundle；旧事件原样保留，reader/upcaster 只能在保留期结束且冷恢复演练后退役。

未经本项目发出的外部手工单默认为 `UNATTRIBUTED`。只有带授权、理由和审计事实的 `ATTACH_EXTERNAL_ORDER` 才能建立关联；不得按价格、数量、时间或策略名猜测归属。

## 5. 持久化、崩溃恢复与放行边界

### 5.1 三层持久化的正确含义

“内存热路径”在定档中指 **持久内存 `PersistentHotJournal`**，不是 Python 对象、队列、callback ring 或普通 DRAM：

```text
已验证 PersistentHotJournal commit
        ↓ 异步复制
SSD 已 seal Journal 段
        ↓ 异步投影
PostgreSQL read model / 对象归档
```

SSD、PG 或归档失败不回溯阻塞已经完成的热 commit、Adapter 调用或网关本地 callback 确认；但 hot 空间、SSD lag 与未 seal LSN 有硬水位，触发时必须 `DISARMED`，不能覆盖唯一事实或无限积压。

每个可启用账户/profile 都绑定已签名的 `AvailabilityObjective`：明确持久内存介质物理损坏的有界异步 SSD RPO，并分别记录本地恢复 `local_recovery_elapsed`、柜台等待 `broker_wait_elapsed` 和包含外部响应的端到端 `recovery_e2e_elapsed`。只有后者可称 RTO；未绑定、未实测、超过目标或恢复证据不完整时，账户不能进入 `READY`，人工 `ARM` 不计入 RTO。

### 5.2 重启恢复的权威顺序

| 问题 | 权威来源 | 恢复规则 |
| --- | --- | --- |
| 本地意图、归属、幂等、冻结、`CALL_STARTED` 因果关系 | 已验证的 Journal 链。 | 已 seal SSD 前缀与连续 hot 后缀合并回放。 |
| 当前订单/成交/撤单、资金持仓与信用状态 | 完整 `BrokerRecoveryEvidence`。 | 回放、只读分页查询和 callback ingress 覆盖共同证明。 |
| PG 事务、checkpoint、运营查询 | 非权威 read model。 | 从 Journal 幂等重建；PG 不能补造事实。 |
| 未提交 frame、易失队列、崩溃中的 SDK 调用栈 | 无可证明事实。 | 保守按 unknown 进入对账，绝不自动重报。 |

恢复固定为：取得 `AccountOwnershipFence` → `BLOCK_NEW / BOOTSTRAP` → 校验 SSD/hot 连续性 → 仅从 Journal 重建易失 OMS → Adapter 挂接 durable ingress、回放和只读快照 → 形成 `BrokerRecoveryEvidence` → 对账。

完全一致时才写入 `RECOVERY_EVIDENCE_CAPTURED + RECOVERY_VALIDATED`，并且最多进入 `DISARMED`；仍需受控人工审批 `ARM` 才能进入 `READY`。任何 LSN gap、半批、未关闭的 callback gap、回放/查询不完整、关联冲突、外部未知单或风险差异，都保持 `RECONCILE_REQUIRED`，禁止自动重报、自动撤单或自动释放冻结。

如果业务 Journal 本身不可验证，只能写独立运维诊断，**不得**向该 Journal 补写 `RECOVERY_GAP` 等“恢复事实”。

若持久内存物理丢失且 SSD 未覆盖完整 hot 后缀，旧 Journal 只读保留、账户持续 `DISARMED`，不得自动对账放行。经柜台完整证据和人工审批后只能建立新的 `journal_id / stream_generation`；旧 cursor 只有在保留 HMAC key、当前授权和独立 `CursorStreamRegistry` 的无环解析都验证成功时才收到无业务载荷的 `JOURNAL_GAP`（无事件、无 next cursor，携带可幂等重试的 `reset_context`，强制取得当前尾流 generation 的快照和 cursor 后才接受）。若控制根不可证明或 reset 图不唯一，保持 `DISARMED + RECONCILIATION` 并返回同样不含业务载荷的 `CURSOR_RECOVERY_UNAVAILABLE`，绝不伪装成普通 cursor 过期。新关联号必须以柜台完整集合重新 seed，绝不从 1 或 PG 水位复用。

## 6. 可插拔柜台：EMT、QMT 与 PaperBroker

EMT 是首个完整 profile，但不是领域模型。`BrokerAdapter` 至少提供 `submit`、`cancel`、`query`、`begin_recovery`、`collect_recovery_evidence` 与 `stop`；`BrokerCapabilities` 还必须声明客户关联回显、稳定原生 ID、回放覆盖、查询完整性、分页终态、as-of/序列与负确认能力。

| Profile | 使用边界 | 启用条件 |
| --- | --- | --- |
| `EmtBrokerAdapter` | 首个生产柜台适配。 | EMT 映射、回调、分页查询、回放与恢复矩阵独立验收。 |
| `QmtBrokerAdapter` | 与 EMT 并列的独立 profile。 | 证明自己的客户关联字段、原生 ID、回放/查询能力；不能复用 EMT 字段、报告或 SDK 类。 |
| `PaperBrokerAdapter` | 非交易时段回归、故障注入、确定性撮合与 300/s 压测。 | 与真实 Adapter 共享领域/恢复契约；结果不冒充柜台性能。 |

EMT 与 QMT 的切换是**受控迁移，不是故障自动切换**。旧 profile 必须先完成回放与对账，且不存在 unknown、working、未归属外部订单或未释放保留；新 profile 使用新的 `venue_id / adapter_instance_id / adapter_client_scope`，在 `DISARMED` 独立启动。新 profile 的空查询永远不能为旧 profile 的未知订单作负确认。

## 7. 性能与验收口径

所有本地性能按 C0–C7 标记：C0 为已完成 binding/schema 校验的策略信号进入账户 ingress，C2 为副作用前 Journal durable，C4 为 Adapter 原生调用返回，C5 为 callback 标量成功进入有界 ingress（EMT 为 `fastspi v2`），C6 为事件 Journal durable，C7 为可 cursor 重放的本地消费者事件。callback 允许先于 C4 返回；此时单独统计 `callback_before_return` 与 C3→C4/C3→C5，绝不生成负 C4→C5 延时。入队失败不计 C5，必须记录 `CallbackGapLatch`、立即 fail-close C1；worker 首次可提交时持久化 `DISARMED + REQUIRED`，且仅在回放/完整查询覆盖后才能结束对账。

- 仅 `C4 <= C5` 样本的 `C4→C5` 才是柜台、网络和 SDK callback 等待，不纳入本地优化目标。
- 热态单笔与 499-burst 是 EMT-only 当前回归口径；它们验证固定 Linux 模拟盘窗口中的完整性与延时，不能外推为生产吞吐 SLA。
- PaperBroker/QMT 的性能结论仍须独立取得真实 Adapter 证据；不得以 EMT 的 499-burst 替代其验收。
- 生产性能结论必须包含 Linux 主机、CPU/绑核、持久内存域、SSD/文件系统、SDK hash、`fastspi_version=v2`、负载定义和 Git SHA；开发/WSL2 样本不得外推生产 SLA。

## 8. 历史交付路线（保留作演进说明）

下表是早期 P0→M5 的设计路线，**不表示当前仍停留在 P0**。当前完成与延期范围以前述权威路线为准。

| 阶段 | 主要成果 | 放行原则 |
| --- | --- | --- |
| P0 | 当前 Gateway、Linux CI、无副作用基线。 | 仅基础设施，不等同 OMS/EMS。 |
| M0 | 领域模型、Consumer/Adapter 契约、映射清单、兼容层。 | 不改变既有 EMT 调用语义。 |
| H0 | 目标 Linux 主机持久内存域、DAX、SSD 与真实恢复能力 Go/No-Go。 | 不通过不开始 M1 的持久内存实现。 |
| M1 | OMS 订单事实、冻结、V2 Journal、异步 SSD、PG read model。 | 崩溃不重复副作用、不重复冻结。 |
| M2 | EMS、完整 EMT profile、`fastspi v2`、保护与可靠事件。 | 仅批准后做低频 EMT 仿真。 |
| M3 | 快照、回放、对账、外部单与人工控制面。 | 对账不通过绝不 `READY`。 |
| M4 | PaperBroker、确定性回归与 300/s 正确性压测。 | 不把仿真结果冒充柜台性能。 |
| M5 | 多消费者、多账户、多 profile、算法、归档恢复。 | 所有隔离与迁移演练通过。 |
| R0 | 目标主机安装、恢复演练、每个 profile 的受控仿真验证、按账户启用。 | M0–M5 和 R0 全部通过才是产品完整落地。 |

## 9. 汇报结论

本项目的核心价值是把策略信号、柜台副作用、异步回调、订单归属和崩溃恢复置于同一条可验证事实链中。当前 EMT-only OMS/EMS 主链路、旁路 OMS 与性能门禁已经验收；下一关键里程碑是保持 `DISARMED` 前提下完成 R1 的同账户多系统权威恢复闭环。PaperBroker、QMT、SDK 全矩阵和跨主机高可用仍须按独立范围继续交付。
