# EMT 交易接口性能文档

> Windows 历史样本仅保留为审计材料，不能作为当前 Linux 支持、性能验收或发布依据；所有后续命令和新增证据均以 Linux x86_64 为准。

## 模拟盘 mutation 的恢复控制

`gateway-mutation` 的真实订单/撤单周期只有在本机 fixture 已武装并显式传入 `--execute-simulated-mutations` 时才会发送委托。若运行在订单确认后、撤单或查询前异常中止，必须先执行 `gateway-mutation-recovery`，而不是再次下单。它查询普通模拟账户的 `QueryUnfinishedOrders`，仅在收到完整终结 callback 后才逐项发起撤单；报告只保存未完成订单数量和撤单结果数量，不保存订单 ID、标的或请求参数。

```bash
.venv/bin/emt-gateway-perf --driver gateway-mutation-recovery --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --profile ordinary --execute-simulated-mutations --output ./artifacts/performance/gateway-mutation-recovery.json
```

恢复控制不计入吞吐/P0 成绩；`query_complete: false` 时必须保持 fail-closed，不允许撤单或开始下一轮 mutation。

### 2026-08-26 普通模拟盘订单/撤单重新验证

一次早期单周期运行暴露 mutation 专用 callback 路由未接收无 payload 查询语义元数据的问题：订单已接受、随后的 `QueryOrderByEMTID` 超时，driver 因此停止在撤单之前。该问题已修复；随后用恢复控制完整查询并撤销 1 笔未完成模拟盘委托，再次核验未完成数量为 0。恢复全过程不保留订单 ID 或业务回包。

修复后先完成 1 个闭环，再以 10 批 × 10 周期、每秒最多一条命令的受控序列扩容；每批结束均运行恢复核验，全部为 0 笔未完成委托。下表为 11 份 payload-free trace 的聚合结果：

| 场景 | 完整成功 / 失败 | C0→C7 p99 (ms) | P0 |
| --- | ---: | ---: | --- |
| `InsertOrder`（普通、稳态） | 101 / 0 | 77.423 | `passed_p0` |
| `CancelOrder`（普通、稳态） | 101 / 0 | 387.519 | `passed_p0` |
| `QueryOrderByEMTID`（普通、稳态） | 202 / 0 | 79.668 | `passed_p0` |
| `OnOrderEvent`（普通、稳态） | 303 / 0 | 16.834 | `passed_p0` |
| `OnQueryOrder`（普通、稳态） | 404 / 0 | 16.969 | `passed_p0` |

这些是 Windows、当前模拟盘和当前 SDK 下的真实 gateway C0–C7 证据。它们重新验证了全矩阵汇总 `gateway-p0-combined-v7` 中已通过的同 5 个场景，故全局 `passed_p0` 计数仍为 140；不构成突发、流控、断线恢复、生产柜台 SLA 或其余账户档案的通过证据。

## 模拟盘信用还款的单次兼容性控制

`gateway-credit-repayment` 只允许本机、已武装 fixture 中的 `CreditCashRepay` 与 `CreditCashRepaySpecifiedDebt`。每项必须指定 `QueryCreditCashRepayInfo` 对账、每秒恰好最多一笔，且单笔金额上限为 1；它在 `C2`、`C3` 前后写入 hash-only journal，以 EMT 返回的操作 ID 关联专用 SPI callback。信用还款 API 没有可由客户端传入的幂等 ID，故此 driver 绝不自动重试；任何 `C3/C4` 后 callback 超时都会先执行只读对账，再停止并要求人工判断，不能以重跑方式消除未知提交。

```bash
.venv/bin/emt-gateway-perf --driver gateway-credit-repayment --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --mutation-fixture ./data/performance/fixtures/emt/emt-mutation-fixture.json --callback-timeout 8 --interval 1 --execute-simulated-mutations --output ./artifacts/performance/gateway-credit-repayment.json
```

### 2026-08-26 信用模拟盘实测

用户已授权在本项目文档中记录模拟盘余额、标的、订单及债务字段（仍不记录账户号、密码、地址或原始回包）。信用快照确认可用资金充足，并选择现金负债 `debt_id=599961`、`order_date=20211231`、`debt_type=0`、标的 `300883`、剩余金额 `73,600` 作为指定债务还款的受控候选；所有本机 fixture、原始快照和 trace 继续位于 Git 忽略目录。

先执行过一次 1 单位直接还款；早期 driver 在随后的对账调用处异常中止，但独立的 `QueryCreditCashRepayInfo` 已确认该笔记录存在，因而没有重试该笔。修复后先以一笔独立还款验证完整链路，再以 1 秒间隔执行 30 笔、每笔 1 单位的直接还款；没有并发、没有自动重试，任一异常均会停止。随后追加 70 次只读 `QueryCreditCashRepayInfo`，使查询 API 达到 100 个完整样本。离线聚合结果如下：

| 场景 | 完整成功 / 尝试 / 失败 | C0→C7 p50 (ms) | p95 (ms) | p99 (ms) | 证据状态 |
| --- | ---: | ---: | ---: | ---: | --- |
| `CreditCashRepay`（信用、稳态） | 30 / 30 / 0 | 32.837 | 1,603.640 | 2,492.042 | `passed_p0` |
| `OnCreditCashRepay`（信用、稳态） | 30 / 30 / 0 | 12.379 | 16.092 | 16.498 | `passed_p0` |
| `QueryCreditCashRepayInfo`（信用、稳态） | 100 / 100 / 0 | 42.298 | 138.549 | 251.231 | `passed_p0`（此前已通过，更新证据） |
| `OnQueryCreditCashRepayInfo`（信用、稳态） | 555 / 555 / 0 | 13.824 | 21.514 | 26.745 | `passed_p0` |

直接还款命令的 p99 为 **2,492.042 ms**，高于其余阶段一个数量级；这是真实模拟盘长尾，后续网关的同步等待预算、队列深度和超时告警必须以此为下界，不得仅以 p50 制定 SLA。本轮使全矩阵的独立 `passed_p0` 场景由 140 增至 143（3 个新场景；`QueryCreditCashRepayInfo` 原已通过）；全局合并报告须在受审计 trace 清单重建时再次验证该计数。

按上述债务字段调用的 `CreditCashRepaySpecifiedDebt` 返回非零原生操作 ID，但 8 秒内未收到 `OnCreditCashRepaySpecifiedDebt`；随后按该债务 ID 的只读核对显示剩余金额仍为 `73,600`。该 fixture 已禁用并标记为 callback/counter blocker，禁止自动重试。

展期路径的只读前置探针 `QueryCreditExtendDebtDateOrders(emt_id=0)` 则在 native 阶段返回 `230113`，SDK 错误码表将其定义为 `EMT_ERR_BIZ_NOT_SUPPORT`（业务不支持）；没有 callback 或可用展期订单。随后对当前信用债务快照逐项核验，12 条记录均为 2021 年形成、2022 年到期，且原始订单 EMT ID 为 0，没有可作为展期请求对象的当前有效债务。因此当前模拟柜台/权限组合不支持信用展期查询，`CreditExtendDebtDate` 不构造字段不成立的请求；新建债务也不能替代缺失的柜台展期查询/对账能力，保留为 capability blocker。

`QueryCreditQuotaTransfer(serial_id=0)` 的初始空结果不再视为“两地节点缺失”的结论。2026-08-27 在用户授权的模拟盘中，以 1 单位、方向 `1` 后方向 `0` 的低频受控配对运行 30 组；60 条变更指令、60 次终结回读均成功，全部配对完成后净方向变动为零。再追加 4 批 × 10 次只读查询，将 `QueryCreditQuotaTransfer` 补足至 100 条完整 C0–C7 trace。四个稳态场景均达到 P0：`CreditQuotaTransfer` 60/60（p50/p95/p99 为 17.179/47.901/369.660 ms）、`OnCreditQuotaTransfer` 60/60（3.576/16.258/18.107 ms）、`QueryCreditQuotaTransfer` 100/100（55.611/123.445/214.144 ms）、`OnQueryCreditQuotaTransfer` 1950/1950（14.894/28.129/32.560 ms）。无重试；该结论仅适用于当前 Windows、本 SDK 与低频模拟盘，不能外推为生产柜台 SLA，也不替代其他 workload 或全矩阵门禁。

### 当前模拟盘的 mutation fixture 决定

按当前指令，只有能由模拟盘快照直接证明其对象、方向和对账路径的变更接口才构造 fixture；无法查询或无法安全制定的项标记为 `skipped`。`skipped` 仅表示本轮模拟盘不执行，矩阵状态仍为 `pending`/blocker，绝不等同于通过或不支持。

| 变更接口 | 当前快照/探针 | 本轮决定 | 原因 |
| --- | --- | --- | --- |
| `InsertOptionCombinedOrder` | 期权账户已以受控的一多一空合约腿建立 `CNSJC` 组合前置；实发组合委托后，专用 callback 与 `QueryOptionCombinedOrderByEMTID` 均成功，后者返回 1 条记录；随后 `QueryOptionCombinedPosition` 返回 1 条记录。 | `compatibility evidence`（1 次受控实发） | 已完成真实 EMT 路径验证；仍需独立、可恢复的样本批次后才可形成 P0 延迟基线。 |
| `InsertOptionExerciseOrder` | 已验证单合约持仓并以 1 份实发行权委托；专用 callback 成功，随后 `QueryPosition` 终结成功并返回 1 条持仓。 | `compatibility evidence`（1 次受控实发） | 已解除“无可行权合约”前置阻断；不以单次委托充当性能基线。 |
| `CreditQuotaTransfer` | 30 组方向 `1`→`0`、各 1 单位的受控恢复对；60 条指令、60 次终结查询均成功，净方向变动归零；追加只读查询后达到 100 条完整查询 trace。 | `passed_p0`（稳态） | 指令、专用 callback、查询及查询 callback 均达到 P0；fixture 已解除，结果仅为低频模拟盘基线。 |
| `CreditExtendDebtDate` | 展期查询返回 `230113 / EMT_ERR_BIZ_NOT_SUPPORT`；12 条现有债务均已于 2022 年到期、原始订单 EMT ID 为 0。 | `skipped`（capability） | 没有有效展期对象，且柜台不提供展期查询/对账能力；禁止用过期债务或虚构 EMT ID 发单。 |

## 1. 范围与验收方法

本页逐项覆盖 `TraderApi` 86 个公开方法。当前供应的 Python binding 缺少 `CreditMarginAvlTransfer`、`QueryCreditMarginAvlTransfer`，因此这两项在取得修复 binding 前只能运行“显式 `unsupported` 拒绝”测试，不能声称真实 EMT 性能通过。接口定义和绑定差异以[接口全覆盖矩阵](../../../02-接口与数据/02-EMT交易与账户接口全覆盖矩阵.md)和[Linux运行环境与SDK兼容性](../../../03-部署运维与可观测性/02-Linux运行环境与SDK兼容性.md)为准。

| 档位 | 测量范围 | 必须满足的标准 |
| --- | --- | --- |
| `LIFE` | `C0→C4`；创建、预登录配置、登录、登出、释放及诊断。 | 单线程、隔离测试账户；报告每次调用和失败码，不与订单热路径混压；失败后实例保持不可交易。 |
| `CMD-H` | `C0→C2→C3→C4`，以及关联 callback 的 `C5→C7`。 | 订单/撤单在稳态、突发、流控和断线恢复场景均报告 p99/p99.9；同订单因果顺序正确；`CALL_STARTED` 后零自动重发。 |
| `CMD-A` | `C0→C2→C3→C4`，以及专用 callback/对账完成。 | 资金、信用、期权操作在授权测试账户串行或经 P0 证明的 lane 下压测；不得占用 `CMD-H` 预算；每次副作用均可关联审计。 |
| `QUERY` | `C0→C1→C3→C4`、首个 callback `C5`、最终 `C6/C7`。 | 覆盖空结果、错误、多页、迟到回调和并发限制；查询限流不得使订单/撤单队列退化。 |

数值门禁不预设微秒或每秒订单数。P0/P4 必须为每个档位、请求负载、账户和平台记录被批准的 `p50/p99/p99.9`、安全速率、最大并发和误差预算；发布时的同条件结果不得超过该已批准预算。柜台返回 `211008` 或等价流控即记录为超过安全速率，触发乘性收缩而不是增加线程。

### 1.1 EMT 单会话报单并发标记与当前优化结论

在本轮单账户、单原生 EMT 会话的 2,000 路原始同时信号模拟盘试验中，柜台接受前 **1,000** 笔委托；其余 1,000 笔在原生调用处同步得到 `211008 / VENUE_THROTTLED`。故本 scope 的 `maximum_concurrent_order_entry_requests` 标记为 **1,000**。这是柜台的实测原始突发拒绝边界，不是可持续订单速率或生产 SLA；各账户、交易日、会话和柜台配置变更后均须重新校准。完整脱敏证据和分段统计见 [`2026-09-05-concurrency-ceiling.json`](../../../../data/performance/probes/emt/tradecore-redis-emt-burst/2026-09-05-concurrency-ceiling.json)。

当前高并发下，优化应按如下顺序进行：

1. **先限制 C0→C1 的排队，而不是移除 Redis。** 300 路/2,000 路突发的 C0→C1 p99 分别为 310.677/1,567.673 ms；该段包含刻意全量同时入队后的本地等待。生产入口应使用固定数量 ingress worker、每账户有界 MPSC 队列和 deadline-aware backpressure；超过动态额度时在 `CALL_STARTED` 前返回 `INGRESS_OVERLOADED`，不能用增大队列把拒绝变成秒级尾延时。
2. **以独立 UDS 执行器验证并消除 submit/callback 的 head-of-line blocking。** 本次性能驱动把两类事实共用“一个串行账户 lane 一条持久连接”的 `UnixSocketRedisCommandExecutor`；试验中 C5→C6 p99 从 300 路的 7.348 ms 升至 2,000 路的 18.636 ms，应先以两条受限连接做 A/B。只有证明每个 Redis Function 的原子 envelope、同账户因果顺序和 callback 快速返回均不变时，才可将它升级为生产组合；不能在 callback 线程直接执行额外同步 I/O。
3. **在原生 EMT 调用之前接入 `EmitAdmissionController`。** 单账户保持单一原生调用 lane；控制器以成功、`211008` 和 callback 延时作为反馈，采用 AIMD、冷却和撤单优先的独立预算。不得并发调用同一 EMT 会话的 `InsertOrder`，不得在 `CALL_STARTED` 后自动重发。
4. **最后做主机侧归因和固定资源。** 以脱敏 `perf`/`ftrace` 记录 ingress worker、native lane、callback normalizer 与 Redis 的 run-queue、softirq 和 GC；在 A/B 证据通过后再设置 CPU 亲和/隔离。C1→C2 p99 仅为 1.093/1.102 ms，Redis gate 不是当前 2,000 路尾延时的首要矛盾，不能为追求微小收益而牺牲 journal-first 副作用边界。

上述第 1、2 项为 P0 代码优化项；变更后须以 300 路、1,000 路和流控保护下的 2,000 路三组实测分别报告 C0→C1、C1→C2、C5→C6 及两段项目内总延时，且不得把 C4→C5 的柜台 callback 等待计入项目内部 SLA。

## 2. 生命周期、会话与诊断（16）

| 接口 | 档位 | 必测负载 |
| --- | --- | --- |
| `CreateTraderApi` | `LIFE` | 冷启动、专用日志目录、创建/释放循环。 |
| `SetCpuAffinity` | `LIFE` | 登录前启用/禁用；核隔离前后对照。 |
| `SetSoftwareVersion` | `LIFE` | 登录前设置与非法值拒绝。 |
| `SetHeartBeatInterval` | `LIFE` | 5–15 秒合法区间及边界值。 |
| `SubscribePublicTopic` | `LIFE` | `RESTART` 订阅、登录后重传。 |
| `RegisterSpi` | `LIFE` | binding 注册映射、重复注册与释放后禁止回调。 |
| `Login` | `LIFE` | 成功、失败、网络超时、重复会话。 |
| `Logout` | `LIFE` | 正常、断线后、日切前；不丢恢复证据。 |
| `Release` | `LIFE` | 仅会话关闭后；释放与迟到 callback 竞争。 |
| `GetTradingDay` | `LIFE` | 登录前/后、日切边界；唯一交易日来源。 |
| `IsServerRestart` | `LIFE` | 健康轮询、服务端重启触发。 |
| `GetApiVersion` | `LIFE` | capability report 与预期版本比对。 |
| `GetApiLastError` | `LIFE` | 主动调用返回 0 后同边界读取。 |
| `GetErrMsgByCode` | `LIFE` | 已知/未知错误码；脱敏诊断。 |
| `GetClientIDByEMTID` | `LIFE` | 归属过滤的缓存未命中与命中。 |
| `GetAccountByEMTID` | `LIFE` | 归属过滤、账号脱敏与登录态限制。 |

## 3. 订单、资产与参考查询（28）

| 接口 | 档位 | 必测负载 |
| --- | --- | --- |
| `InsertOrder` | `CMD-H` | 稳态、突发、同键重试、流控、callback 先到。 |
| `CancelOrder` | `CMD-H` | 同订单因果、重复撤单、部分成交/撤回。 |
| `QueryOrderByEMTID` | `QUERY` | 单订单、空结果、错误、迟到 callback。 |
| `QueryOrders` | `QUERY` | 过滤条件、空/多结果、并发限制。 |
| `QueryUnfinishedOrders` | `QUERY` | 登录恢复和高订单数。 |
| `QueryOrdersByPage` | `QUERY` | 多页、`query_reference`、尾页。 |
| `QueryTradesByEMTID` | `QUERY` | 单订单成交、空/多成交。 |
| `QueryTrades` | `QUERY` | 多结果、重复成交去重。 |
| `QueryTradesByPage` | `QUERY` | 多页、页序列、尾页。 |
| `QueryPosition` | `QUERY` | 单标的、市场边界、空持仓。 |
| `QueryPositionByPage` | `QUERY` | 多页与迟到页。 |
| `QueryAsset` | `QUERY` | 单次响应、错误与当日字段。 |
| `QueryFund` | `QUERY` | 单次响应；不得等待 `is_last`。 |
| `FundTransfer` | `CMD-A` | serial ID、失败、断线后对账。 |
| `QueryFundTransfer` | `QUERY` | 划拨日志的分页/空结果。 |
| `QueryOtherServerFund` | `QUERY` | 单次响应与跨节点失败。 |
| `QueryETF` | `QUERY` | 多 item、完成标记。 |
| `QueryETFTickerBasket` | `QUERY` | 大篮子、多 item、背压。 |
| `QueryETFByPage` | `QUERY` | 页序列与参考字段。 |
| `QueryIPOInfoList` | `QUERY` | 多 item、权限拒绝。 |
| `QueryIPOQuotaInfo` | `QUERY` | 空额度、单次/完成语义。 |
| `QueryOptionAuctionInfo` | `QUERY` | 期权参考数据与错误。 |
| `QueryIssueInfoList` | `QUERY` | 多 item、交易日边界。 |
| `QuerySecurityInfo` | `QUERY` | 单证券、非法代码。 |
| `QuerySecurityByPage` | `QUERY` | 多页、空页和尾页。 |
| `QueryHKConnectIndicativeExchangeRateInfo` | `QUERY` | 单次响应；不得伪造 `is_last`。 |
| `QueryHkConnectSpreadTable` | `QUERY` | 表规模、权限及错误。 |
| `QueryYesterdayAsset` | `QUERY` | 日切前后与单次完成。 |

## 4. 融资融券（27）

| 接口 | 档位 | 必测负载 |
| --- | --- | --- |
| `CreditCashRepay` | `CMD-A` | 金额边界、返回 EMT ID、回调/对账。 |
| `CreditCashRepaySpecifiedDebt` | `CMD-A` | 指定负债、拒绝与断线。 |
| `CreditSellStockRepayDebtInterestFee` | `CMD-A` | 订单/成交 callback 关联。 |
| `CreditExtendDebtDate` | `CMD-A` | 债务 ID 保序与回调。 |
| `CreditQuotaTransfer` | `CMD-A` | serial ID、方向与回调。 |
| `CreditMarginAvlTransfer` | `CMD-A` | 当前 binding 仅验证 `unsupported`；修复后测 serial ID、方向、回调。 |
| `QueryCreditCashRepayInfo` | `QUERY` | 多 item、完成标记。 |
| `QueryCreditFundInfo` | `QUERY` | 单次响应。 |
| `QueryCreditDebtInfo` | `QUERY` | 多债务、完成标记。 |
| `QueryCreditDebtInfoByPage` | `QUERY` | 分页、页序列。 |
| `QueryCreditTickerDebtInfo` | `QUERY` | 标的过滤、空结果。 |
| `QueryCreditAssetDebtInfo` | `QUERY` | 单次响应。 |
| `QueryCreditTickerAssignInfo` | `QUERY` | 多 item。 |
| `QueryCreditTickerAssignInfoByPage` | `QUERY` | 分页。 |
| `QueryCreditExcessStock` | `QUERY` | 单次响应。 |
| `QueryMulCreditExcessStock` | `QUERY` | `is_last` 位于非统一参数位置。 |
| `QueryCreditExtendDebtDateOrders` | `QUERY` | 多 item、订单 ID。 |
| `QueryCreditFundExtraInfo` | `QUERY` | 单次响应。 |
| `QueryCreditPositionExtraInfo` | `QUERY` | 多 item。 |
| `QueryCreditPledgeStkRate` | `QUERY` | 费率规模、错误。 |
| `QueryCreditMarginRate` | `QUERY` | 费率规模、错误。 |
| `QueryCreditPositionFullRate` | `QUERY` | 费率规模、错误。 |
| `QueryCreditPledgeStkByPage` | `QUERY` | 分页。 |
| `QueryCreditTargetStkByPage` | `QUERY` | 分页。 |
| `QueryCreditDebtToRepayTodayByPage` | `QUERY` | 分页、日切。 |
| `QueryCreditQuotaTransfer` | `QUERY` | 划拨日志、分页/完成。 |
| `QueryCreditMarginAvlTransfer` | `QUERY` | 当前 binding 仅验证 `unsupported`；修复后测分页/完成。 |

## 5. 期权组合与终端准入（15）

| 接口 | 档位 | 必测负载 |
| --- | --- | --- |
| `InsertOptionExerciseOrder` | `CMD-A` | 授权、返回 ID、回调和对账。 |
| `InsertOptionCombinedOrder` | `CMD-A` | 组合 ID、回调和并发 lane。 |
| `QueryOptionCombinedUnfinishedOrders` | `QUERY` | 恢复与空结果。 |
| `QueryOptionCombinedOrderByEMTID` | `QUERY` | 单订单与错误。 |
| `QueryOptionCombinedOrders` | `QUERY` | 多 item。 |
| `QueryOptionCombinedOrdersByPage` | `QUERY` | 分页。 |
| `QueryOptionCombinedTradesByEMTID` | `QUERY` | 单订单成交。 |
| `QueryOptionCombinedTrades` | `QUERY` | 多 item。 |
| `QueryOptionCombinedTradesByPage` | `QUERY` | 分页。 |
| `QueryOptionCombinedPosition` | `QUERY` | 持仓规模、空结果。 |
| `QueryOptionCombinedStrategyInfo` | `QUERY` | 单/多 item 与权限。 |
| `QueryOptionCombinedExecPosition` | `QUERY` | 行权持仓与错误。 |
| `QueryEndpointId` | `QUERY` | 终端准入、单次/错误。 |
| `QueryDMATickerAssignInfoByPage` | `QUERY` | 分页、权限。 |
| `QueryBlackWhiteListByPage` | `QUERY` | 分页、空页、尾页。 |

## 6. 主动调用的发布证据

每次发布至少交付：每个上述接口的覆盖状态、对应 workload、调用与回调直方图、调用返回码、错误/流控计数、队列水位、CPU/内存/GC、SDK/环境指纹及已批准预算比较。未开通接口可为 `unsupported`，但必须包含稳定拒绝路径的性能记录，且绝不能被从报告或 catalog 中删除。
