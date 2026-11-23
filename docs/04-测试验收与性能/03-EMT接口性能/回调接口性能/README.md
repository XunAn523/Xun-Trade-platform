# EMT 回调接口性能文档

> Windows 历史样本仅保留为审计材料，不能作为当前 Linux 支持、性能验收或发布依据；所有后续命令和新增证据均以 Linux x86_64 为准。

## 1. 共同标准

本页逐项覆盖 `TraderSpi` 69 个回调。每个 callback 都必须测量 `C0→C5`（进入 native/Python callback 到最小副本入 ring）、`C5→C6`（归一化与 durable）和需要策略可见时的 `C6→C7`。callback 内只能复制最小字段、分配 ring 槽位并返回；禁止数据库、文件、网络、JSON/FlatBuffers 编解码、策略代码或等待锁。

| 档位 | 优先级 | 必须满足的标准 |
| --- | --- | --- |
| `CB-H` | 高 | 订单、成交、撤单错误、会话断开及账户副作用。稳态、突发、乱序/重复和慢策略同时压测；零未解释丢失，ring 硬水位即 `DISARMED`。 |
| `CB-A` | 高 | 资金、信用、期权和持仓变化。必须保留原生 ID、错误及账户归属；不允许挤占订单/成交队列。 |
| `CB-Q` | 低 | 查询 item。压测分页、空结果、错误和迟到 callback；可限流查询，但原生副本、审计与完成语义不可静默丢失。 |
| `CB-S` | 会话 | 连接和通用错误。压测断线风暴、重复连接/断开、恢复期间 callback；不得自动恢复下单。 |

`CB-H`/`CB-A` 必须报告 p99.9，`CB-Q` 必须报告 p99；所有档位都报告最大值、ring 水位、队列等待、持久化失败和 CPU/GC。数值以目标机、目标 SDK、测试账户和已批准 `performance-baseline.json` 为准；没有批准基线不得写入固定微秒承诺。

## 2. 连接、订单、账户与副作用（16）

| 回调 | 档位 | 必测负载 |
| --- | --- | --- |
| `OnConnected` | `CB-S` | 重连循环、重复通知。 |
| `OnDisconnected` | `CB-S` | 断线风暴、恢复闸门和迟到事件。 |
| `OnError` | `CB-S` | 错误风暴、告警 outbox 背压。 |
| `OnOrderEvent` | `CB-H` | 新单、拒绝、撤回、乱序、重复、callback 先于返回。 |
| `OnTradeEvent` | `CB-H` | 多成交、重复 `exec_id`、自成交、慢策略。 |
| `OnCancelOrderError` | `CB-H` | 撤单失败、原单/撤单 ID 关联。 |
| `OnFundTransfer` | `CB-A` | serial ID、失败、断线后对账。 |
| `OnCreditCashRepay` | `CB-A` | EMT ID、错误与归属。 |
| `OnCreditCashRepaySpecifiedDebt` | `CB-A` | 指定债务、错误与归属。 |
| `OnCreditExtendDebtDate` | `CB-A` | 债务 ID 保序。 |
| `OnCreditQuotaTransfer` | `CB-A` | serial ID、方向与归属。 |
| `OnCreditMarginAvlTransfer` | `CB-A` | 当前 binding 仅验证缺失拒绝；修复后测回调风暴与 serial ID。 |
| `OnOptionExerciseOrderEvent` | `CB-A` | 行权状态变化与错误。 |
| `OnOptionCombinedOrderEvent` | `CB-A` | 组合订单乱序/重复。 |
| `OnOptionCombinedTradeEvent` | `CB-A` | 组合成交去重与慢策略。 |
| `OnHoldingChangeEvent` | `CB-A` | 高频持仓变化；不得误当行情流。 |

## 3. 普通账户查询回调（19）

| 回调 | 档位 | 必测负载 |
| --- | --- | --- |
| `OnQueryOrder` | `CB-Q` | 多 item、`is_last`、错误。 |
| `OnQueryOrderByPage` | `CB-Q` | 页序列、`query_reference`、尾页。 |
| `OnQueryTrade` | `CB-Q` | 多 item、重复成交。 |
| `OnQueryTradeByPage` | `CB-Q` | 页序列和迟到页。 |
| `OnQueryPosition` | `CB-Q` | 多持仓、空结果。 |
| `OnQueryPositionByPage` | `CB-Q` | 分页、空页。 |
| `OnQueryAsset` | `CB-Q` | 单/多响应、错误。 |
| `OnQueryFund` | `CB-Q` | 单次响应；不得等待 `is_last`。 |
| `OnQueryFundTransfer` | `CB-Q` | 划拨日志、多 item。 |
| `OnQueryOtherServerFund` | `CB-Q` | 单次响应、跨节点错误。 |
| `OnQueryETF` | `CB-Q` | 多 item、背压。 |
| `OnQueryETFBasket` | `CB-Q` | 大篮子、多 item。 |
| `OnQueryETFByPage` | `CB-Q` | 分页与尾页。 |
| `OnQueryIPOInfoList` | `CB-Q` | 多 item、权限。 |
| `OnQueryIPOQuotaInfo` | `CB-Q` | 额度为空、完成。 |
| `OnQueryOptionAuctionInfo` | `CB-Q` | 多 item、错误。 |
| `OnQueryEndpointId` | `CB-Q` | 单次响应、准入失败。 |
| `OnQueryDMATickerAssignInfoByPage` | `CB-Q` | 分页。 |
| `OnQueryBlackWhiteListByPage` | `CB-Q` | 分页、空页和尾页。 |

## 4. 融资融券查询回调（25）

| 回调 | 档位 | 必测负载 |
| --- | --- | --- |
| `OnQueryCreditCashRepayInfo` | `CB-Q` | 多 item、完成。 |
| `OnQueryCreditFundInfo` | `CB-Q` | 单次响应。 |
| `OnQueryCreditDebtInfo` | `CB-Q` | 多债务、完成。 |
| `OnQueryCreditDebtInfoByPage` | `CB-Q` | 分页。 |
| `OnQueryCreditTickerDebtInfo` | `CB-Q` | 标的过滤、空结果。 |
| `OnQueryCreditAssetDebtInfo` | `CB-Q` | 单次响应。 |
| `OnQueryCreditTickerAssignInfo` | `CB-Q` | 多 item。 |
| `OnQueryCreditTickerAssignInfoByPage` | `CB-Q` | 分页。 |
| `OnQueryCreditExcessStock` | `CB-Q` | 单次响应。 |
| `OnQueryMulCreditExcessStock` | `CB-Q` | `is_last` 特殊参数位置。 |
| `OnQueryCreditExtendDebtDateOrders` | `CB-Q` | 多 item。 |
| `OnQueryCreditFundExtraInfo` | `CB-Q` | 单次响应。 |
| `OnQueryCreditPositionExtraInfo` | `CB-Q` | 多 item。 |
| `OnQueryCreditPledgeStkRate` | `CB-Q` | 费率规模。 |
| `OnQueryCreditMarginRate` | `CB-Q` | 费率规模。 |
| `OnQueryCreditPositionFullRate` | `CB-Q` | 费率规模。 |
| `OnQueryCreditPledgeStkByPage` | `CB-Q` | 分页。 |
| `OnQueryCreditTargetStkByPage` | `CB-Q` | 分页。 |
| `OnQueryCreditDebtToRepayTodayByPage` | `CB-Q` | 分页、日切。 |
| `OnQueryCreditQuotaTransfer` | `CB-Q` | 划拨日志。 |
| `OnQueryCreditMarginAvlTransfer` | `CB-Q` | 当前 binding 仅验证缺失拒绝；修复后测分页/完成。 |
| `OnQueryYesterdayAsset` | `CB-Q` | 日切、单次响应。 |
| `OnQueryIssueInfoList` | `CB-Q` | 多 item。 |
| `OnQuerySecurityInfo` | `CB-Q` | 单证券、错误。 |
| `OnQuerySecurityByPage` | `CB-Q` | 分页、空页。 |

## 5. 期权组合与港股通查询回调（9）

| 回调 | 档位 | 必测负载 |
| --- | --- | --- |
| `OnQueryOptionCombinedOrders` | `CB-Q` | 多 item、完成。 |
| `OnQueryOptionCombinedOrdersByPage` | `CB-Q` | 分页。 |
| `OnQueryOptionCombinedTrades` | `CB-Q` | 多 item、去重。 |
| `OnQueryOptionCombinedTradesByPage` | `CB-Q` | 分页。 |
| `OnQueryOptionCombinedPosition` | `CB-Q` | 持仓规模、空结果。 |
| `OnQueryOptionCombinedStrategyInfo` | `CB-Q` | 多 item。 |
| `OnQueryOptionCombinedExecPosition` | `CB-Q` | 行权持仓。 |
| `OnQueryHKConnectIndicativeExchangeRateInfo` | `CB-Q` | 单次响应；不得伪造 `is_last`。 |
| `OnQueryHkConnectSpreadTable` | `CB-Q` | 表规模、错误。 |

## 6. 回调发布证据

每次发布对每个回调交付：触发路径、覆盖状态、`C0→C5/C6/C7` 直方图、ring/worker 队列水位、失败与丢弃原因、归属/去重结果、审计 LSN、告警 outbox 证据及基线比较。查询 callback 的限流只能拒绝新的查询；已接收的原生 callback 仍须有可解释的原始记录和完成/超时结论。

## 7. 受控本地回调注入（开发证据，不是 EMT 性能证据）

当真实柜台暂时无法稳定产生某类非稳态事件时，可运行 `gateway-callback-injection` 检查网关自身的 `CallbackPipeline`：本地断开/连接通知、有限突发、重复到达、成对乱序到达、C5 后慢消费者、空查询终态和两页查询回调形状。注入只传递 callback 名称、账户档案、workload 与必要的空/分页布尔和整数标记；不加载 SDK、不连接网络、不使用账户、订单或原始 callback payload。默认强制 native fastspi，并以 write-through `FileJournal` 记录 C6 与 `journal_durable_lag_ns`；`--callback-injection-journal memory --allow-python-fastspi` 仅用于本地功能调试，不得用于性能判断。

```bash
.venv/bin/emt-gateway-perf --driver gateway-callback-injection --fixture-owner fengyu --callback-injection-iterations 100 --callback-injection-burst-width 4 --callback-injection-delay-ms 10 --output ./artifacts/performance/injected/gateway-callback-injection.json
```

输出 JSON 与自动生成的 `.injected.traces.jsonl` 必须保存在 `artifacts/performance/injected/`，与真实 EMT 柜台 trace 分开。该 trace 首行是 payload-free 的 `artifact_classification`，正式汇总器会在 P0 计算前拒绝其 `controlled_gateway_injection` 类型。报告固定声明 `counter_performance_eligible: false`、`release_gate_eligible: false` 和 `must_not_merge_with_emt_trace_evidence: true`：其 C0–C7 时延只能用于定位本机 ring、归一化、内存 journal 与 ACK 路径的回归，不能说明柜台吞吐、网络断线恢复、供应商回调乱序/重复，亦不能让相应 callback 的真实 P0 或发布门禁通过。重复/乱序注入只验证当前管线未丢失且完成 ACK；去重、业务序号校验和消费者重排仍须通过真实 payload schema 与 EMT fixture 单独验收。

## 8. 真实会话断线 fixture（普通模拟账户）

`gateway-session-fault` 在 Linux 使用独立代理进程，避免 EMT 同步 `Login()` 持有 GIL 时阻塞同进程 Python proxy 的 accept/forward。代理只复位本 fixture 的 SDK/upstream socket 对，并可在有界 blackout 窗口内复位新连接；它不调用查询、下单、撤单或资金接口，也不修改系统防火墙或路由。完整报告通过并发 drain 从 child pipe 读取，避免 capability matrix 大于 pipe buffer 时被错误判为 worker timeout。

默认 `loopback_proxy` 将 SDK Login 指向 loopback。`transparent_redirect` 是更严格的 Linux-only 复核：SDK 仍接收真实 EMT host/port；仅该 throw-away fixture 进程启动前加载的 connect shim，才将**精确匹配**的 AF_INET connect 定向到固定 loopback port。代理子进程设置 bypass 后直连真实上游，因此不会形成重定向环路；shim 不安装到系统路径、不会修改全局路由，进程结束后即失效。

当前 EMT `2.27.0.0` Linux binding 的正式验收标准为 `emt_2_27_client_rst_auto_reconnect_v1`：隔离 native worker、transparent redirect、SDK API version 和两项 SHA-256 必须精确匹配已定档版本；随后 `proxy_disconnect_injected`、`transport_reconnected` 与恢复 `native_on_connected` 必须均为真。`native_on_disconnected` 继续逐轮记录，但当前版本的客户端侧 RST 样本不以它为通过前提。二进制漂移、普通 loopback、未隔离 worker、初始连接失败或任一传输/恢复事实缺失均不得通过。正常 `logout` 不构成断线证据。

2026-09-14 Linux 复核依次排除了：child report pipe deadlock、vendor wrapper finalizer abort、同进程 proxy/GIL 互等、初始 `OnConnected` 被误配对、FIN 弱故障，以及 loopback endpoint 特殊性。版本绑定复测 `t6-session-semantics-c37f2f1-r2.json` 使用 `TraderApi.getApiVersion()=2.27.0.0`、`libemt_api.so=6eb8c354…d4c9` 和 `vnemttrader.so=d15c6343c…3e9e`，确认 initial `OnConnected`、proxy RST、真实 transport reconnect 与恢复 `OnConnected`，`OnDisconnected=false`。最终受控复验 `t6-session-semantics-10acf96-r4.json` 精确匹配同一 API version/hash，并写入 `acceptance_complete=true`；它是上述版本化客户端 RST 自动重连验收的**通过证据**。SDK 随附 C++ 手册仍声明通信断开应回调 `OnDisconnected`，而同包 Python sample/FAQ 又要求用户手工 `Login()`；完整边界见 [EMT 2.27.0.0 会话重连语义判定](../../../03-部署运维与可观测性/05-EMT-2.27.0.0-会话重连语义判定.md)。不得用本地 callback 注入、`Logout` 或主动 `Login` 伪造任一事实。

当前标准下，客户端 RST 自动重连已经验收通过；EMT 的服务端主动断连、断线风暴和 `OnDisconnected` 完整回调序列是**未覆盖的扩展场景**，不能被当前样本外推。它们不阻断本项版本化验收；如未来要求其覆盖，仍需 EMT 提供单 session 服务端断连/恢复控制，或书面确认等价、可验证完成语义。无论采用何种标准，session-fault 通过后仍只进入 `RECOVERED_DISARMED`，不得自动 `READY`。
