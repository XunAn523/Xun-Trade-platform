# TradeCore 与 EMT 接口、恢复及多柜台适配

> 用途：EMT 接口与恢复架构汇报；早期 P0/M 阶段表述只用于说明演进，不作为当前验收状态。
> 当前状态（2026-09-11）：EMT-only F0、F1、F2、F3、F4、F6、OMS W7 及 R1 同账户多系统的受控 manual-only 恢复演练已完成；R1 仍为 `RECOVERED_DISARMED`，全部账户保持 `DISARMED`，不构成 `READY` 或通用自动恢复证明。当前结论见[未实施功能与优化收口路线](../01-架构与定档/10-当前未实施功能与优化收口路线.md)。
> 对接范围：EMT `TraderApi` 主动调用与 `TraderSpi` 异步回调；不包含 `QuoteApi` 行情。
> 定位：EMT 是首个柜台 profile，不是策略或 OMS 的直接依赖；QMT 及未来柜台必须通过同一 Adapter 契约独立接入。

## 1. EMT 在目标产品中的位置

完整链路中，EMT 被封装在 `EmtBrokerAdapter` 内：

| 层次 | 对 EMT 的职责 | 不允许的行为 |
| --- | --- | --- |
| 消费者策略 | 经 `ConsumerGatewayPort` 提交绑定信号、读取自己的回执和事件。 | 直接登录 EMT、直接调用 SDK、伪造账户/策略身份。 |
| OMS | 保存订单事实、归属、冻结和状态，产生 `ExecutionInstruction`。 | 用 PG 授权柜台副作用，或把同步返回值视为终态。 |
| EMS | 路由至账户 execution lane，做保护与会话监督，并调用 Adapter。 | 绕过 Journal、为恢复自动重报、在回调线程查询柜台。 |
| `EmtBrokerAdapter` | 将领域指令映射为 EMT 请求，规范化回调、查询与恢复证据。 | 把 EMT Python 类型、返回码或枚举泄漏到 OMS/策略。 |

当前 EMT-only 已具备分片 session、Journal-first 订单/回调事实、`fastspi v2`、OMS/EMS 路由和受控 Linux 模拟盘验收；R1 的权威查询终态、真实 R2 断点和双系统隔离已完成受控演练，但证书仍因有限 callback witness 保持 `full_r1_acceptance=false`。PaperBroker/QMT 是独立延期范围。

## 2. 通用 Adapter 契约与 EMT 映射

领域层不使用 `order_emt_id` 作为通用模型，而使用：

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

| 通用字段 | EMT profile 映射 | 用途 |
| --- | --- | --- |
| `venue_id` | `EMT` | 与 QMT、PaperBroker 的事实和恢复证据隔离。 |
| `adapter_client_scope` | `client_id` / EMT client scope | 限定客户关联号的唯一性范围。 |
| `client_order_ref` | `order_client_id` | 本项目对 EMT 委托的稳定客户关联号。 |
| `venue_order_id` | `order_emt_id` | EMT 接受/回报后的原生订单 ID。 |
| `venue_cancel_id` | `order_cancel_emt_id` | EMT 撤单接受/回报后的原生撤单 ID。 |

`order_client_id` 必须由 EMS 在其 EMT profile 的账户、交易日和 client scope 内分配为非零、单调且不复用的 `uint32`；策略、CLI 和人工界面都不能自定该数值。策略名称、员工姓名、备注等文本只保存于本地归属事实，绝不编码进 32 位关联号。

每个 Adapter 必须实现或声明：`submit`、`cancel`、`query`、`begin_recovery`、`collect_recovery_evidence`、`stop`，以及 `BrokerCapabilities`。能力清单必须明确客户关联回显、稳定原生 ID、公共流回放、查询完整性、分页终态、as-of/序列和负确认能力；未被证实的能力不可被 OMS 假设为可用。mapping/capability/恢复声明只有经本地 `BundleTrustPolicy` 信任根验签、版本与 release manifest 一致时才可加载；EMT 自身字段或 SDK 回包不能绕过该控制面。

## 3. EMT 会话、账户所有权与状态

每个 `(account_id, venue_id, adapter_client_scope)` 在指定 Linux 主机上只允许一个活跃 Adapter session 和一个活动 Journal generation。`AccountOwnershipFence` 必须先取得独立于 Journal 根目录、服务账户专属的 `GatewayControlRoot/AccountControlRoot` 中 `flock(LOCK_EX | LOCK_NB)`，再创建 SDK、登录、回放或进行任何 Adapter 副作用；因此 PMem 损坏后的新 generation 仍与旧 generation 竞争同一把锁，不能双登录或双写。网关级控制根保存全局 `ReleaseState`，账户子目录只保存锁、cursor HMAC key 和 stream-reset registry，绝不作为订单事实源；除锁诊断外，控制记录必须同目录临时写入、file/目录 `fsync` 后原子 rename，成功落盘前不得加载 release、签发 cursor 或发布新 generation。

EMT 的恢复启动顺序为：

```text
取得 AccountOwnershipFence
→ 加入 RECONCILIATION AdmissionGate / BOOTSTRAP
→ 校验本地 Journal 并恢复易失状态
→ RegisterSpi / attach durable callback ingress
→ SubscribePublicTopic(RESTART)
→ Login
→ 回放 + 当日订单/成交/未完成订单/资产/持仓/信用只读查询
→ BrokerRecoveryEvidence + Reconciler
→ DISARMED
→ 人工审批 ARM 后才可 READY
```

锁冲突、SDK/ABI 不匹配、回调 ring 溢出、Journal 失败、登录/查询失败、外部未知单或任一对账差异属于恢复性故障：均会停止新单并转为 `DISARMED + REQUIRED`，开始恢复才进入 `RECONCILING`。风险数据过期只加入 `RISK_STALE` 保护性 gate，刷新并验证快照后才可解除。TCP 连通、登录成功、一次查询成功或 PG 恢复均不表示账户可以下单。

## 4. 下单与撤单：先事实、后副作用

### 4.1 EMT `InsertOrder`

1. `ConsumerGateway` 校验消费者、账户、策略 binding；OMS 以 `(strategy_id, strategy_order_id, intent_hash)` 做持久幂等。
2. OMS 在同一个 `PersistentHotJournal` 原子批次写入 `ORDER_ATTRIBUTED`、`RESERVATION_CREATED`、`CALL_STARTED`。任何 append、加密、flush/drain 或 commit marker 失败，都不得调用 `InsertOrder`。
3. EMS 在该账户执行 lane 内写入 EMT `order_client_id`，再经 `EmtBrokerAdapter.submit` 调用 `InsertOrder`。
4. C4 返回先到时，返回非零 `order_emt_id` 后必须先把 `NATIVE_ACCEPTED` 及当时已知的 `venue_order_id` 耐久化；在此之前不能向调用方宣称原生受理。若同一订单 callback 先到，则先以 `NATIVE_OBSERVED_FROM_CALLBACK + BROKER_EVENT` 耐久化，迟到返回只记录 `NATIVE_RETURN_OBSERVED`，不得回退状态、重复冻结或重复投递。
5. 返回值不明、进程在调用边界崩溃或 `NATIVE_ACCEPTED` 无法提交时，保守进入 `SUBMIT_UNKNOWN → RECONCILE_REQUIRED`，绝不自动重报。

### 4.2 EMT `CancelOrder`

撤单有独立、稳定的 `cancel_request_id`。`CANCEL_REQUESTED + CANCEL_STARTED` 必须在跨越 Adapter `cancel` 前同批耐久化；重复请求只返回原 request，不能重复发撤单。EMT 返回的 `order_cancel_emt_id` 作为 `venue_cancel_id` 以 `CANCEL_NATIVE_ACCEPTED` 耐久化。

`OnCancelOrderError` 映射为 `CANCEL_REJECTED`。撤单返回不明为 `CANCEL_SUBMIT_UNKNOWN`；只有权威订单事实证明仍在 `WORKING / PARTIALLY_FILLED` 时，才可能由新的显式流程再次申请撤单。结果不明、未归属或未挂接外部订单不得自动撤单或释放冻结。

## 5. EMT `TraderSpi` 回调与 C5–C7

回调是异步事实入口，不是直接执行策略代码的入口。当前 EMT-only 性能路径为 `fastspi v2`：SDK 绑定边界只复制固定标量到预分配 native POD MPSC ring，slot 仅包含 callback kind、`producer_id + callback_seq`、单调时间、EMT 关联字段、状态/数量、成交去重键和有限长度 UTF-8 错误字节。

后置 worker 执行：

```text
规范化 BrokerEvent
→ 按 source_key 去重、归属与状态转换
→ Event Journal durable（含成交/冻结调整）
→ 生成可 cursor 重放的 ConsumerEvent
→ 异步 PG projector / 可选 JetStream 扇出
```

回调线程禁止 SQL、网络、策略执行、字典/dataclass 构造、UUID、指标/trace、EMT 反查和消费者投递。缺失 `order_client_id` 的 EMT 回调如需 `GetClientIDByEMTID`，只能由后置、限流、单飞的 lookup worker 处理；超时、冲突或失败必须保持 `UNKNOWN` 并进入对账。

ring 满时 producer 不重试、不阻塞、不覆盖 slot，也不把 callback 静默丢掉：必须以原生原子操作更新账户级 `CallbackGapLatch`（每个 producer 的首/末拒绝序号、计数和时间），立即 fail-close C1 并加入 `CALLBACK_GAP + RECONCILIATION` AdmissionGate。容量恢复后 worker 首先写 `CALLBACK_GAP_DETECTED`、`DISARMED + REQUIRED` 状态事实和消费者可见的 `ACCOUNT_STATUS`；若在此之前崩溃，恢复把该 ingress 区间视为未证明。只有已验证的 RESTART 回放与完整查询覆盖、比较并关闭全部 gap 后，账户才可能 `RECOVERY_VALIDATED → DISARMED`。

同步 `InsertOrder`/`CancelOrder` 返回只表示调用结果；订单有效、成交、撤成或拒绝只能由规范化 `BrokerEvent`、查询和对账共同确认。

## 6. 查询、公共流与恢复证据

EMT 查询遵循“调用成功不等于结果完整”的规则。每种查询声明版本化 `QueryCompletionSpec`，明确关联模式、`is_last`/单回调/分页终态、空结果语义、cursor、最大页数与超时。没有被 SDK 与实测共同证明的单回调，不能当作全量结果。

恢复中的 `BrokerRecoveryEvidence` 对 EMT 至少包含：

- 已通过 durable ingress 接收的 `SubscribePublicTopic(RESTART)` 回放覆盖；
- 每个 `CallbackGapEvidence` 的序号域、缺口范围及其回放/查询覆盖关闭证据；
- 当日订单、成交、未完成订单及资产、持仓、适用信用查询的完整结果；
- 账户、交易日、分页终态、as-of/序列（若柜台提供）、本地 ingress LSN 窗口与 SDK/mapping/capability 版本；
- 每项结果的完整性结论，而不是将空列表当作“没有订单”。

本地事实由已验证的“SSD 已 seal 前缀 + 连续持久内存后缀”回放；柜台对当前订单、成交和风险输入具有权威性；PG 仅是可重建 read model。该恢复合同只有在完整查询证书、回放覆盖与稳定双扫描均成立时才允许写入 `RECOVERY_VALIDATED` 并重建易失 OMS 到 `DISARMED`。当前 R1 尚未取得 position 查询终态与 `RESTART` replay 的实证，因此必须保持 `RECOVERY_BLOCKED`；任何情况下都不自动进入 `READY`。

LSN gap、半批、密钥/anchor 错误、未关闭或崩溃前仅内存可见的 callback gap、回放/查询缺页、未回显关联字段、窗口不稳定、数量差异或外部未知单都会保持 `RECONCILE_REQUIRED`。若业务 Journal 不可验证，诊断必须写到独立运行证据，禁止向它追加恢复事实；任何情况下都不自动重报、自动撤单或以 PG 补造事实。

若持久内存物理损坏且 SSD 未覆盖最后 hot LSN，柜台只可帮助确认当前可观察状态，不能补回丢失的策略归属、冻结因果或消费者历史。旧 Journal 因此停止写入；经完整柜台证据与人工决定后创建新的 `journal_id / stream_generation`。新 Journal 根可读、registry durable、`ACCOUNT_STREAM_RESET` durable 后，旧 cursor 才能经稳定 `AccountControlRoot` 中保留 HMAC key、当前 binding/授权与 `CursorStreamRegistry` 的无环尾流解析验证，收到携带可幂等重试 `reset_context`、无业务载荷且无 next cursor 的 `JOURNAL_GAP`，再取得当前尾流快照。控制根、key 或 registry 在保留期内不可证明或 reset 图不唯一时，账户保持 `DISARMED + RECONCILIATION` 且接口返回同样不含业务/流定位字段的 `CURSOR_RECOVERY_UNAVAILABLE`，不能降级为 `CURSOR_EXPIRED`。EMT `order_client_id` 只能在柜台完整订单/成交/未完成订单集合已证明后，按最大已知值以上重新 seed；无法证明时保持 `DISARMED`。

## 7. 普通现金与两融能力门禁

普通现金、融资买入、融券卖出均为独立 capability。每次准入前的 `DecisionContext` 必须同时固定 account capability、Adapter/mapping manifest、风险快照、规则包和本地 feature flag 版本；其中任一项缺失、过期或 hash 不一致，OMS 必须在冻结和 `CALL_STARTED` 前拒绝新单。

| 能力 | 关键额外输入 | 真实 EMT 仿真验收边界 |
| --- | --- | --- |
| 普通现金 | 资金、可卖、价格保护、交易时段。 | 最小白名单样本，回调、查询和清理可对照。 |
| 融资买入 | 融资额度、保证金、标的资格、费用/保证金模型。 | 柜台确认能力合格后，最小样本完成订单、回调、查询和额度快照核验。 |
| 融券卖出 | 券源、可融数量、保证金、可卖规则、标的资格。 | 柜台确认能力和券源后，最小样本完成订单、回调、查询和券源/持仓核验。 |
| 突发性能 | EMT-only 已有 499-burst Linux 模拟盘证据。 | 不能替代两融、QMT/PaperBroker 或生产柜台的独立性能验收。 |

真实 EMT 样本只证明当时接口的受控语义、回调/查询归属和清理结果，不能由少量样本推导 p99/p99.9、吞吐或生产可用性。

## 8. QMT 与未来柜台的接入规则

`QmtBrokerAdapter` 只能实现同一 `BrokerAdapter` 与 `VenueOrderIdentity`，并独立提交 SDK hash、mapping manifest、回调 source-key 向量、查询完成语义、回放覆盖与崩溃恢复矩阵。QMT 不得复用 EMT session、SPI、枚举数值、`order_client_id` 假设或 EMT 验收报告。

EMT→QMT 或 QMT→EMT 不是自动 failover。迁移前，旧 profile 必须对账完成且没有 `CALL_STARTED/CANCEL_STARTED` 结果不明、working 订单、外部未知单或未释放保留；新 profile 使用新的身份 scope 并从 `DISARMED` 启动。新柜台的空查询不能为旧柜台订单提供负确认。

## 9. 汇报结论

EMT 集成的正确性不在于“某个 SDK 方法能返回”，而在于把请求、原生 ID、回调、查询、策略归属、冻结和崩溃恢复组织为同一条可验证事实链。EMT-only OMS/EMS 主链路、W7 与 R1 manual-only 权威恢复演练已完成；外部 Adapter、SDK 全矩阵、全面 callback replay 语义和生产适用性仍未完成。任何模拟盘委托都不能替代这些独立结论。
