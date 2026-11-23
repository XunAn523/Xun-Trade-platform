# EMT 全交易接口 OMS / EMS 实施与验收定档

> 状态：**实施基线（T0--T6）**。范围固定为 EMT API `2.27` 的全部 `TraderApi` 与 `TraderSpi` 交易/账户能力；明确不包含 `QuoteApi`、行情订阅、行情缓存、行情数据模型和行情策略接口。本定档不改变现有 `MmapJournalProfileV1`、三 scope/process EMT execution 分片、`fastspi v2` native ingress、Control callback 默认路径及 `DISARMED` 结论。

## 1. 目标与边界

目标不是把任意 EMT Python 方法反射暴露给策略，而是将官方 `TraderApi` 86 项方法与 `TraderSpi` 69 项回调逐项纳入强类型、能力受限、可恢复、可审计的 OMS / EMS 交易模块。

下列内容在范围内：普通证券报撤和查询、资金划拨、融资融券订单/还款/展期/额度与保证金调拨、期权行权/组合策略、ETF/IPO/证券/港股通交易参考查询、DMA/黑白名单、会话/错误/持仓变动通知及全部对应回调。

下列内容不在范围内：`QuoteApi`，逐笔/盘口/行情订阅，行情缓存，行情推送，行情策略 API，或把 `TraderApi` 的按需交易参考查询伪装成行情流。

## 2. 不可协商的热路径约束

```text
Fast execution lane
strategy → C0 → route/risk → PRE_NATIVE mmap → CALL_STARTED mmap
         → one native SDK caller → accepted mmap → C4
         → native callback ingress(C5) → callback Journal(C6) → C7

Control mutation lane
fund / credit repayment / extension / quota / margin transfer / option exercise
         → bounded + rate-limited + Journal-first state machine

Query lane
all Query* → request-id / cursor / last semantics → immutable OMS snapshot
```

1. 每个 EMT session 只有一个 SDK caller；多 session 吞吐仅由独立 scope / process 分片提供。
2. 普通 `InsertOrder`、`CancelOrder`、`CreditSellStockRepayDebtInterestFee`、期权组合策略报单是 Fast execution lane 的成员；期权行权与资金/信用管理操作在独立 control lane。
3. control/query lane 的锁、CPU、I/O、队列、对象构建、日志、审计、数据库和慢消费者不得进入 C0--C7。
4. 所有 `TraderSpi` 回调只复制固定标量到 `fastspi v2` 有界 native ring；SDK callback 线程不得同步写 Journal、访问 PG/HTTP、序列化 JSON 或执行策略代码。
5. callback ingress 继续采用现有 Control writer。2026-09-14 的 direct-write candidate 虽消除了局部 `C5A→C5B` 等待，但完整 `C5→C7` P95/P99 回退，故不得默认启用。

## 3. 统一目录、能力与状态模型

`TraderApiCatalogV1` 是 86 个官方方法的唯一静态目录；`TraderSpiCatalogV1` 是 69 个官方回调的唯一静态目录。每一项至少定义：官方名、Python binding 名、业务域、lane、请求/回调 schema、权限、预期关联键、终态规则、是否副作用、回退/对账规则。

接口状态只允许：

- `supported_verified`：binding、强类型适配、本地合同及真实模拟盘证据均通过；
- `supported_contract_only`：代码/合同通过但没有真实可验证证据；
- `unsupported_binding`：当前 Linux Python binding 缺失；
- `unsupported_account` / `unsupported_venue`：账户或模拟柜台明确未提供；
- `blocked_semantics`：不能证明回调终态、分页、关联或恢复语义。

任何未列项、未知 binding、未知 callback、未授权能力或未完成语义都 fail-closed；不得保留“默认成功”的 `pending` 调用路径。

统一持久化语义为：

```text
OPERATION_INTENT
→ PRE_NATIVE
→ CALL_STARTED
→ NATIVE_ACCEPTED / NATIVE_REJECTED
→ NATIVE_CALLBACK or AUTHORITATIVE_QUERY
→ OPERATION_TERMINAL / RECONCILE_REQUIRED
```

订单类以 `(scope, epoch, order_client_id, order_emt_id)` 关联；资金、额度、保证金等操作以 `(scope, epoch, operation_id, serial_id)` 关联；查询以 `(scope, session_epoch, request_id, query_kind, cursor)` 关联。`CALL_STARTED` 后严禁自动重试。

## 4. T0--T6 工作包

| 包 | 交付 | 对热路径影响 | 包完成后的统一验收 |
| --- | --- | --- | --- |
| T0 | 86/69 静态目录、binding ABI diff、capability manifest、所有 API/SPI 的显式状态 | 无 | 静态差异、目录完备性、未知项 fail-closed |
| T1 | 全 SPI bridge、会话/错误/持仓事件、优先级与 callback schema | 仅 C5 最小标量复制 | 69 callback 注册/签名/overflow/unknown/late 合同，499 回归 |
| T2 | 普通证券全查询、订单/成交按 EMT ID 查询、资金划拨及其状态机 | 查询/control lane | 查询终态/分页/serial 对账、499 非干扰 |
| T3 | 两融全操作、负债/券源/费率/额度查询、专用 callback/serial state machine | 订单报撤保持 Fast；其余 control | 两融最小真实样本、错误/恢复/清理、499 非干扰 |
| T4 | 期权行权、组合策略、全部组合查询/回调/状态机 | 组合报单 Fast；行权 control | 组合订单/成交/撤余、行权时间门禁、499 非干扰 |
| T5 | OMS/EMS 统一投影、恢复、权限、API 与审计组合 | 只消费已提交事实 | replay/namespace/ID 冲突/恢复矩阵、混合旁路压力 |
| T6 | 全矩阵真实 capability、性能基线、发布 evaluator | 无新增路径 | 逐项证据、3 轮性能、故障/清理/发布门禁 |

## 5. T0：目录和 ABI 能力门禁

T0 从官方 2.27 清单导出固定名称集合，读取实际 Linux Python binding 的可调用属性与文件/ABI 哈希，并生成可签名 `trading-capability-report.json`。目录不得使用“任意字符串 + 任意结构体”的反射透传。

当前已知的供应包硬缺口是 `CreditMarginAvlTransfer`、`QueryCreditMarginAvlTransfer`、`OnCreditMarginAvlTransfer`、`OnQueryCreditMarginAvlTransfer`。在供应商交付可验证 binding 前，它们只能为 `unsupported_binding`，并在 API 层返回稳定 `CAPABILITY_UNAVAILABLE`。

## 6. T1：全回调桥与会话状态

69 个 callback 全部必须在 native bridge 具有显式 override，并以固定 callback code 注册 `fastspi v2`。回调按以下优先级进入独立有界队列：

1. P0：订单、成交、撤单错误、期权订单/成交、资金/信用 mutation 结果、连接断开和错误；
2. P1：持仓变动；
3. P2：全部查询 item / page / terminal。

`OnDisconnected`、callback gap、未知 callback、跨 scope ID、callback 终态缺失均关闭新单并进入 `RECOVERY_BLOCKED`。`OnConnected` 不等于自动恢复或 `READY`。

## 7. T2--T4：交易域接入

### 7.1 普通证券与资金划拨

普通域除现有报撤外，必须公开按 EMT ID 的订单/成交查询，并适配订单、成交、持仓、资产、资金、ETF、IPO、证券、港股通、DMA 与黑白名单查询。`FundTransfer` 使用独立 serial 状态机，只有 callback 与 `QueryFundTransfer` 对账后才 terminal。

### 7.2 两融

融资买入/融券卖出沿用 `InsertOrder` 的订单热路径。现金还款、指定负债还款、卖券还款、展期、额度/保证金调拨使用 control mutation lane。全部债务、券源、可用量、担保品、费率、额度、待还和调拨日志查询进入 query lane。每项都需要专用 ID/serial、callback、查询和故障恢复合同。

### 7.3 期权

组合策略报单进入 scope-local Fast execution lane，行权进入 control mutation lane。组合订单、成交、未完成订单、持仓、策略定义与行权持仓查询均保留分页/终态语义；期权 callback 与普通订单 callback 共用 native ingress，但在 OMS 中保持独立 state machine。

## 8. T5：OMS / EMS、恢复与权限

每个 operation 进入 Journal-first 事实流，再异步投影为 OMS immutable snapshot。EMS 只产生强类型 intent，不能直接接触 SDK 结构体。权限至少区分 `trade.order`、`trade.cancel`、`account.transfer`、`credit.*`、`option.*`、`account.query.*` 与 `native.callback.read`。

恢复必须使用 scope/epoch 绑定的 authority query，不能以缓存或 PG 推断；未知 native 副作用、callback gap、分页不完整、external ID 冲突或 namespace 归属不明均为 `RECONCILE_REQUIRED`。当前产品结论仍为 `RECOVERED_DISARMED`，不授予自动恢复或 `READY`。

## 9. T6：验收与发布

每项接口都需要 binding 合同、schema/权限负例、真实模拟盘能力探针，以及适用的成功、拒绝、空结果、分页、超时、迟到 callback、断线和重启证据。对于账户或柜台不支持的能力，保留可验证的 `unsupported_*` evidence，而不是盲目重试。

每个完整功能包结束后统一执行：全量回归、3 轮热态单笔、3 轮 `TC-EMT-499`、callback 子段全配对、清理 terminal、权威 `QueryUnfinishedOrders=0`、OMS mixed load 及故障/恢复矩阵。新增 control/query 能力不得使冻结 Control profile 的 `C0→C4A`、`C5→C7` P95/P99 发生无解释回退。

发布 evaluator 只有在 86/69 目录无未分类差异、所有可声明能力拥有相应证据、当前 binding 缺口显式禁用、热路径回归通过且账户仍处于明确审批状态时，才允许输出 `release-ready`；它不自动改变账户为 `READY`。

## 10. 实施原则

1. 复用仓库现有 `emt_gateway` 的官方 API facade、完整 SPI bridge、session、query registry 和 `fastspi v2` 代码，不复制第二套 vendor 绑定逻辑；新核心只能通过 `xuanling_tradecore.compatibility.emt_trading_platform` 这一受控边界引用它，禁止 `xuanling_tradecore.adapters` 直接 import 遗留包或 vendor SDK。
2. 新的 `xuanling_tradecore.adapters.emt` 只负责强类型目录、OMS/EMS operation lane、Journal/恢复合同及 capability gate；旧平台层在正式组合完成前不得删除。
3. 所有代码修改按工作包提交至 `dev`；服务器只使用 Git 拉取部署。真实模拟盘 fixture 在每轮完成后解除 armed，报告不保存敏感业务 payload。

## 11. 2026-09-14 实施与验收状态

本节只记录最终事实，不把“代码存在”写成“柜台能力已验证”。本轮 T6 代码提交 `efea4a7`、`0edecef`、`612a5bb`、`ab49d8c`、`69d0d00`、`b84e5c1`、`9deb978`、`1725bd3`、`aeadc24` 与 `86a88c6` 均已由服务器 `/home/fengyu/tradecore-acceptance-499` 通过 `git pull --ff-only` 部署；部署只使用 Git 拉取，不上传源码或 SDK。

期权不再作为当前普通/信用产品迭代的发布前提。`emt-ordinary-credit-v1` 以 scope 门禁在 SDK 前拒绝期权域调用，完整 catalog 和全接口证据仍保留；普通/信用扩展验收与外部 blocker 见[普通与信用账户发布范围与验收定档](14-普通与信用账户发布范围与验收定档.md)。

| 工作包 | 代码与合同状态 | 当前证据/结论 |
| --- | --- | --- |
| T0 | **已完成（目录、ABI gate 与显式拒绝）**：`TraderApiCatalogV1`/`TraderSpiCatalogV1` 固定 86/69 目录、lane/domain 分类、未知项 fail-closed、ABI attestation 工具 | Linux 无登录 ABI probe：`TraderApi` 86 项中 84 项为 direct/adapter-owned，`TraderSpi` bridge 69/69 override 完备。供应包没有导出 `CreditMarginAvlTransfer`、`QueryCreditMarginAvlTransfer` 及两个对应 callback；它们固定返回 `CAPABILITY_UNAVAILABLE/unsupported_binding`，不能用 ctypes 或伪造 SPI ABI 冒充完成。 |
| T1 | **已完成（静态组合）**：69 callback kind 与 fastspi v2 gap-latch 注册表逐项校验，生成 bridge 后逐项检查 override | C5 不增加 wrapper、锁、日志或对象投影。完整回调的真实模拟盘逐项语义、overflow、late callback 回归保留在 T6 统一验收，未提前写成已通过。 |
| T2 | **已完成（合同与最小真实 mutation）**：全 `Query*` 入口继续经 request-id/terminal registry；`FundTransfer` 经 committed-fact 控制状态机 | 普通只读 14/14 以各自 request-id 收到终态 callback。真实模拟盘完成一组 `FundTransfer` 的正反向各一次：两次 native accepted、两次 `QueryFundTransfer` 终态对账成功、无自动重试；报告不含资金、账户或 serial。 |
| T3 | **已完成（合同与可执行两融子集）**：两融还款、展期、额度/保证金调拨的命名控制面，卖券还款继续 Fast lane；两融查询只走 query lane | 真实模拟盘完成一组 `CreditQuotaTransfer` 的正反向各一次：两次 callback 与两次 `QueryCreditQuotaTransfer` 终态对账成功。16 项只读中 15 项终态；`QueryCreditPositionFullRate` 收到 1 个 `is_last=false` callback 后未再终结，故为 `blocked_semantics`。供应包缺失的保证金调拨仍不可调用。 |
| T4 | **已完成（合同、标准期权订单/撤单与查询分域）**：期权组合报单保持 Fast lane，行权使用 control lane，查询按域隔离 | 实时只读选择的有效期权完成 1 笔被动限价 `InsertOrder → CancelOrder`，两步均 native accepted 且各自 `QueryOrderByEMTID` 对账终态成功。期权只读 10 项中 8 项终态；`QueryOptionCombinedOrdersByPage`、`QueryOptionCombinedTradesByPage` 均同步返回 `6077`，按 SDK 错误码表归类为 `unsupported_venue`。组合/行权仍没有当前可验证的策略或行权候选，因此保持 `supported_contract_only`。 |
| T5 | **已完成（代码/合同）**：非订单控制操作进入独立 committed-fact OMS sidecar；权限精确到 `trade`、`account`、`credit`、`option` 与 callback read，不支持通配符 | sidecar 只消费已提交事实，不能反向阻塞 C0--C7。真实 restart/replay/namespace 混合矩阵在 T6 前仍为待验收。 |
| T6 | **已完成（证据工具、最小 mutation、性能基线与故障 supervisor）**：逐项 evidence evaluator、ABI attestation、Git 生成的 40 项只读 fixture、一次性进程报告器和 native-fault supervisor | Linux ABI probe：86 API 中 84 项 direct/adapter-owned，69 SPI override 全部存在，4 项 binding 缺口显式禁用。真实只读矩阵为 40 项：37 项终态、1 项 `blocked_semantics`、2 项 `unsupported_venue`。三轮热态单笔与三轮 499-burst 已通过；EMT `2.27.0.0` 精确二进制的 transparent-RST 自动重连标准已通过，仍保持 `RECOVERED_DISARMED`。**`all_trading_capabilities_verified=false`，不得标记为完整交易模块 READY。** |

### 11.1 真实只读验收的可复核结果

服务器工件为 `artifacts/t6-query-fixture-{ordinary,credit,option}-86a88c6.json` 与汇总 `artifacts/t6-query-acceptance-86a88c6.json`；它们只保留 profile、API 名、终态计数和稳定错误分类，不保留账户、证券、订单、资金、密码、地址或 callback payload。每个完成的只读进程在报告落盘后使用 `--isolate-after-report` 退出；该隔离只能证明报告已固化，**不能**被用作 SDK 正常 `Logout/Release` 的证据。

| profile | 只读项 | 终态 callback 已验证 | 阻断 | 结论 |
| --- | ---: | ---: | --- | --- |
| ordinary | 14 | 14 | 无 | 已启用只读合同均有终态；`QueryUnfinishedOrders` 的账户总数只作共享账户基线，不可据此识别或撤销其他系统委托 |
| credit | 16 | 15 | `QueryCreditPositionFullRate`：1 条 `is_last=false` 后 8 秒内没有终态 | `blocked_semantics`；这是 EMT 返回语义缺口，不是 Python bool 解析问题 |
| option | 10 | 8 | 两个分页 API：同步原生 `6077` | SDK 错误码表定义为 `unsupported_venue`，不盲目重试 |
| 合计 | 40 | 37 | 1 个语义阻断、2 个柜台不支持 | 不满足 T6 全能力 release gate |

### 11.2 最小真实 mutation、热路径与清理证据

`FundTransfer` 与 `CreditQuotaTransfer` 均使用一对严格串行的正反向最小动作（每秒至多一条、无自动重试）；每个方向都必须 native accepted、接收对应 callback，并以其专属查询拿到 `terminal_seen=true` 才能执行下一方向。期权使用实时只读选择出的仍有效合约，只执行一笔被动限价单及**该返回订单**的撤单；不存在泛账户撤单。

499-burst 使用三个独立 Python 进程、三个 EMT API/session 和每 session 一个 serial native caller，在共同 C0 同时释放 499 条信号。三轮完整通过为 `t6-499-burst-ab49d8c-r1.json`、`t6-499-burst-69d0d00-r3.json` 与 `t6-499-burst-69d0d00-r4.json`：每轮均 `499/499` native accepted、`499/499` C7、`499/499` 撤单受理、零 callback gap 与零本轮 cleanup inflight。三轮的 `C0→C4` P95 分别为 `10.529/10.503/10.360 ms`，`C5→C7` P95 为 `8.162/5.775/10.631 ms`，`C0→C7` P95 为 `63.479/63.875/59.829 ms`；整批 `C0→last C7` 为 `63.983/64.597/60.489 ms`。中间一次未作为通过样本的 R2 出现 1 条撤单终态 callback 缺失；其后账户级 `QueryUnfinishedOrders` 总数保持 `1→1`。由于模拟账户可被其他系统共用，不能将这个账户级数值冒充为本系统订单或据此做泛撤单。

三轮热态单笔均为 100 样本、全局 submission overlap 为 0，且每笔在下一笔前均已完成 C7 与 own cancel terminal：每轮均 `100/100` native accepted、`100/100` C7、`100/100` cleanup terminal。`C0→C4` P95 为 `0.471/0.483/0.481 ms`，`C5→C7` P95 为 `0.602/0.540/0.576 ms`，端到端 `C0→C7` P95 为 `13.529/9.954/12.413 ms`。这些是当前 Linux 三分片构成的实测结果，不外推为柜台承诺。

### 11.3 Session-fault 与发布边界

`a2a62fc` 后的 session-fault runner 使用独立 Linux proxy process、并发 drain child report 与 payload-free worker milestones；它消除了旧 `worker_timeout_terminated` 的 pipe-buffer 误判，也将 native GIL 与 proxy accept/forward 解耦。透明重定向复核 `artifacts/t6-session-fault-a2a62fc-transparent.json` 保持 SDK Login 的真实 EMT host/port，只把该 throw-away child 的精确 connect 重定向到 sibling proxy；没有下单、撤单、查询、资金/信用动作，也没有全局网络变更。

该一轮 RST + 12 秒 blackout 已确认 initial `OnConnected`、proxy disconnect、transport reconnect 与恢复 `OnConnected`，但 native `OnDisconnected` 未出现。相同结果也出现在非透明 loopback fixture；因此不是 proxy 地址、GIL、回调管线或 child report 生命周期问题。依据当前定档，API `2.27.0.0` 与已记录两项 Linux SDK hash 的 isolated transparent-RST 样本以传输重连和恢复 `OnConnected` 为验收边界，故该场景通过。服务器 `t6-session-semantics-10acf96-r4.json` 已写入 `acceptance_complete=true`；它只证明客户端 RST 自动重连。通过后仍为 `RECOVERED_DISARMED`，禁止自动恢复、自动撤单、自动重报和 `READY`。服务端主动断线、断线风暴和完整 `OnDisconnected` 序列转为未覆盖扩展场景；如需覆盖，仍须 EMT 提供可控的单 session fixture 或等价、可验证完成语义，且不得以本地注入或 `Logout` 替代。

本轮本地完整回归、服务器定向回归以及 ABI/query acceptance 工具都必须通过，才可更新本节；回归证明组合边界、报告完整性与 fail-closed 行为，不替代其余 mutation、故障恢复和三轮热路径性能证据。

当前 revision `6ab3fd1` 的完整回归已执行：本机 Windows CPython 3.10 `.venv` 为 `828 passed, 22 skipped, 1 warning, 84 subtests passed in 28.88s`；Linux Git 部署环境为 `836 passed, 14 skipped, 1 warning, 84 subtests passed in 17.24s`。两端唯一 warning 均为 `testcontainers.postgres` 弃用告警；Windows/Linux skip 数差异来自平台条件与可用测试依赖，不是失败。Linux 测试夹具会根据平台生成严格的 SDK hash 文件集合，运行时校验仍保持 Windows 三文件、Linux 两文件的 fail-closed 规则。
