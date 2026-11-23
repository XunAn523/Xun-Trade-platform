# EMT 交易与账户接口全覆盖矩阵

> 基线：EMT API `2.27.0`，以官方 `TraderApi` / `TraderSpi` 文档为准。  
> 范围：覆盖 `TraderApi` 的全部 86 个公开方法（85 个实例方法和 `CreateTraderApi`）与 `TraderSpi` 的全部 69 个回调；不包含行情 `QuoteApi`。  
> 目标：每个原生交易/账户接口都有显式 Python binding 适配、权限与能力门禁、审计记录、错误处理、回调归一化和测试项；不得以“未用到”静默跳过。

> 供应包状态：仓库内 `EMT_python_SDK/emt_api_python` 不是此目标的全覆盖证据。其 Linux Python binding 未暴露 2.27 新增的 `creditMarginAvlTransfer` / `queryCreditMarginAvlTransfer` 及两个对应回调，并保留废弃的 `StructuredFund` 名称；这些差异在获得修复 Linux binding 前必须报告为 `unsupported`。详见[Linux运行环境与SDK兼容性](../03-部署运维与可观测性/02-Linux运行环境与SDK兼容性.md)。

官方清单：[TraderApi](https://emt.eastmoneysec.com/api/help/classEMT_1_1API_1_1TraderApi.html)、[TraderSpi](https://emt.eastmoneysec.com/api/help/classEMT_1_1API_1_1TraderSpi.html)。`GetApiVersion()` 的运行时结果、供应方交付 Python binding 的文件哈希、Python ABI 与本页版本必须共同写入 capability report；不允许只根据网页版本假定实际绑定兼容。官方文档说明 SPI 回调需要快速返回，否则可能阻塞后续消息并导致断线；本项目的所有回调均遵循“最小复制入 ring，后置处理”的共同约束。

## 1. 全覆盖的定义与实现规则

1. `native_api.py` 建立不可变 `NativeMethodSpec`：每个官方方法的原始名称、请求 schema、返回/错误规则、权限、能力标志、关联 ID 提取器、预期回调和持久化策略均为显式条目，禁止通过“任意方法名 + 任意结构体”反射透传。
2. `fastspi` 必须实现本页列出的每一个 `TraderSpi` override。每次回调都生成 `NativeCallbackRecord(callback_name, payload, error, request_id, is_last?, page_meta, session_id, observed_at)`；`is_last` 仅在原生签名提供时存在，`callback_name` 与必要原生字段永不丢失。
3. Normalizer 同时生成两种输出：`NATIVE_CALLBACK`（完整、版本化、脱敏的原生语义）和面向普通订单流程的派生事件（如 `ORDER_EVENT`、`TRADE_EVENT`）。任何无法映射到派生事件的回调仍必须投递 `NATIVE_CALLBACK`、写 Event/Audit Journal、记录日志和指标。
4. 查询类回调以 `(session_epoch, request_id, query_kind)` 归并；每个 item 保留 `callback_name`，分页字段 `req_count`、sequence、`query_reference` 不可丢失；`is_last` 或官方单次响应规则产生 `ACCOUNT_QUERY_COMPLETE`。
5. 全覆盖不等于所有策略默认拥有权限。每个方法受 `CapabilityPolicy`、账户开通状态、策略权限和风控审批控制；若 SDK Python binding/账户未支持，capability report 必须明确列为 `unsupported`，被调用时返回稳定 `CAPABILITY_UNAVAILABLE`，绝不伪造成功或吞掉回调。
6. 所有原生调用、回调、查询分片、分页、错误、拒绝和能力拒绝均写 `FlowRecord`；WARN+ 同时写结构化日志和 `AlertOutbox`，由企微机器人可靠投递。

## 2. `TraderApi` 86 个公开方法覆盖

### 2.1 生命周期、会话与诊断（16）

| 官方方法 | Gateway 适配职责 | 对策略的可见语义 |
| --- | --- | --- |
| `CreateTraderApi` | 创建唯一原生实例，记录 SDK 路径、版本、日志目录。 | 仅 `SESSION_EVENT`。 |
| `SetCpuAffinity` | 登录前可选性能配置；能力与核隔离检查。 | 仅 Ops 审计。 |
| `SetSoftwareVersion` | 登录前设置发布版本。 | 仅 Ops 审计。 |
| `SetHeartBeatInterval` | 登录前设置心跳。 | 会话健康指标。 |
| `SubscribePublicTopic` | 登录前订阅本交易日订单/成交流重传。 | `SESSION_EVENT` / 恢复回调。 |
| `RegisterSpi` | 注册全量 `fastspi` 实现。 | 不直接暴露。 |
| `Login` | 唯一同步生命周期调用，获得 `session_id`。 | `SESSION_EVENT(LOGINNING/...)`。 |
| `Logout` | 受控断线/日切退出。 | `SESSION_EVENT`。 |
| `Release` | 只在会话关闭后释放。 | `SESSION_EVENT`。 |
| `GetTradingDay` | 唯一交易日来源。 | 脱敏会话状态。 |
| `IsServerRestart` | 健康探测和恢复触发器。 | `SESSION_EVENT`。 |
| `GetApiVersion` | capability report 必填。 | 只读 Ops。 |
| `GetApiLastError` | 同一调用边界读取 0 返回的错误。 | 规范化错误/告警。 |
| `GetErrMsgByCode` | 原生错误码解释；结果仅作脱敏诊断。 | `native_message`。 |
| `GetClientIDByEMTID` | 订单/账户类回调归属过滤。 | 不直接暴露。 |
| `GetAccountByEMTID` | 订单/账户类回调账户归属过滤。 | 不直接暴露真实账号。 |
> `GetTradingDay` 是唯一交易日来源，禁止用本机日期代替；这是实现约束，不是额外 SDK 方法。

### 2.2 普通证券订单、成交、持仓与资产（13）

| 官方方法 | canonical `operation_kind` / `query_kind` | 关联回调 |
| --- | --- | --- |
| `InsertOrder` | `SECURITY_ORDER` | `OnOrderEvent`、`OnTradeEvent`。 |
| `CancelOrder` | `CANCEL_ORDER` | 成功由 `OnOrderEvent`，失败由 `OnCancelOrderError`。 |
| `QueryOrderByEMTID` | `ORDER_BY_EMT_ID` | `OnQueryOrder`。 |
| `QueryOrders` | `ORDERS` | `OnQueryOrder`。 |
| `QueryUnfinishedOrders` | `UNFINISHED_ORDERS` | `OnQueryOrder`。 |
| `QueryOrdersByPage` | `ORDERS_BY_PAGE` | `OnQueryOrderByPage`。 |
| `QueryTradesByEMTID` | `TRADES_BY_EMT_ID` | `OnQueryTrade`。 |
| `QueryTrades` | `TRADES` | `OnQueryTrade`。 |
| `QueryTradesByPage` | `TRADES_BY_PAGE` | `OnQueryTradeByPage`。 |
| `QueryPosition` | `POSITION` | `OnQueryPosition`。 |
| `QueryPositionByPage` | `POSITION_BY_PAGE` | `OnQueryPositionByPage`。 |
| `QueryAsset` | `ASSET` | `OnQueryAsset`。 |
| `QueryFund` | `FUND` | `OnQueryFund`；单次响应，无 `is_last`。 |

### 2.3 资金划拨、ETF、IPO、期权基础信息与证券基础信息（15）

| 官方方法 | canonical `operation_kind` / `query_kind` | 关联回调 |
| --- | --- | --- |
| `FundTransfer` | `FUND_TRANSFER` | `OnFundTransfer`。 |
| `QueryFundTransfer` | `FUND_TRANSFER_LOG` | `OnQueryFundTransfer`。 |
| `QueryOtherServerFund` | `OTHER_SERVER_FUND` | `OnQueryOtherServerFund`。 |
| `QueryETF` | `ETF` | `OnQueryETF`。 |
| `QueryETFTickerBasket` | `ETF_TICKER_BASKET` | `OnQueryETFBasket`。 |
| `QueryETFByPage` | `ETF_BY_PAGE` | `OnQueryETFByPage`。 |
| `QueryIPOInfoList` | `IPO_INFO_LIST` | `OnQueryIPOInfoList`。 |
| `QueryIPOQuotaInfo` | `IPO_QUOTA_INFO` | `OnQueryIPOQuotaInfo`。 |
| `QueryOptionAuctionInfo` | `OPTION_AUCTION_INFO` | `OnQueryOptionAuctionInfo`。 |
| `QueryIssueInfoList` | `ISSUE_INFO_LIST` | `OnQueryIssueInfoList`。 |
| `QuerySecurityInfo` | `SECURITY_INFO` | `OnQuerySecurityInfo`。 |
| `QuerySecurityByPage` | `SECURITY_BY_PAGE` | `OnQuerySecurityByPage`。 |
| `QueryHKConnectIndicativeExchangeRateInfo` | `HK_CONNECT_INDICATIVE_RATE` | `OnQueryHKConnectIndicativeExchangeRateInfo`。 |
| `QueryHkConnectSpreadTable` | `HK_CONNECT_SPREAD_TABLE` | `OnQueryHkConnectSpreadTable`。 |
| `QueryYesterdayAsset` | `YESTERDAY_ASSET` | `OnQueryYesterdayAsset`。 |
| `QueryOptionAuctionInfo` 以外的期权委托 | 见 2.5。 | — |
| `OnHoldingChangeEvent` 对应持仓变动通知 | 原生推送，无主动调用。 | 见第 3 节。 |

### 2.4 融资融券交易、资金、负债与额度（27）

| 官方方法 | canonical `operation_kind` / `query_kind` | 关联回调 |
| --- | --- | --- |
| `CreditCashRepay` | `CREDIT_CASH_REPAY` | `OnCreditCashRepay`。 |
| `CreditCashRepaySpecifiedDebt` | `CREDIT_CASH_REPAY_SPECIFIED_DEBT` | `OnCreditCashRepaySpecifiedDebt`。 |
| `CreditSellStockRepayDebtInterestFee` | `CREDIT_SELL_REPAY_DEBT_INTEREST_FEE` | `OnOrderEvent` / `OnTradeEvent`；无专用 SPI。 |
| `CreditExtendDebtDate` | `CREDIT_EXTEND_DEBT_DATE` | `OnCreditExtendDebtDate`。 |
| `CreditQuotaTransfer` | `CREDIT_QUOTA_TRANSFER` | `OnCreditQuotaTransfer`。 |
| `CreditMarginAvlTransfer` | `CREDIT_MARGIN_AVL_TRANSFER` | `OnCreditMarginAvlTransfer`。 |
| `QueryCreditCashRepayInfo` | `CREDIT_CASH_REPAY_INFO` | `OnQueryCreditCashRepayInfo`。 |
| `QueryCreditFundInfo` | `CREDIT_FUND_INFO` | `OnQueryCreditFundInfo`。 |
| `QueryCreditDebtInfo` | `CREDIT_DEBT_INFO` | `OnQueryCreditDebtInfo`。 |
| `QueryCreditDebtInfoByPage` | `CREDIT_DEBT_INFO_BY_PAGE` | `OnQueryCreditDebtInfoByPage`。 |
| `QueryCreditTickerDebtInfo` | `CREDIT_TICKER_DEBT_INFO` | `OnQueryCreditTickerDebtInfo`。 |
| `QueryCreditAssetDebtInfo` | `CREDIT_ASSET_DEBT_INFO` | `OnQueryCreditAssetDebtInfo`。 |
| `QueryCreditTickerAssignInfo` | `CREDIT_TICKER_ASSIGN_INFO` | `OnQueryCreditTickerAssignInfo`。 |
| `QueryCreditTickerAssignInfoByPage` | `CREDIT_TICKER_ASSIGN_INFO_BY_PAGE` | `OnQueryCreditTickerAssignInfoByPage`。 |
| `QueryCreditExcessStock` | `CREDIT_EXCESS_STOCK` | `OnQueryCreditExcessStock`。 |
| `QueryMulCreditExcessStock` | `CREDIT_MULTI_EXCESS_STOCK` | `OnQueryMulCreditExcessStock`。 |
| `QueryCreditExtendDebtDateOrders` | `CREDIT_EXTEND_DEBT_DATE_ORDERS` | `OnQueryCreditExtendDebtDateOrders`。 |
| `QueryCreditFundExtraInfo` | `CREDIT_FUND_EXTRA_INFO` | `OnQueryCreditFundExtraInfo`。 |
| `QueryCreditPositionExtraInfo` | `CREDIT_POSITION_EXTRA_INFO` | `OnQueryCreditPositionExtraInfo`。 |
| `QueryCreditPledgeStkRate` | `CREDIT_PLEDGE_STOCK_RATE` | `OnQueryCreditPledgeStkRate`。 |
| `QueryCreditMarginRate` | `CREDIT_MARGIN_RATE` | `OnQueryCreditMarginRate`。 |
| `QueryCreditPositionFullRate` | `CREDIT_POSITION_FULL_RATE` | `OnQueryCreditPositionFullRate`。 |
| `QueryCreditPledgeStkByPage` | `CREDIT_PLEDGE_STOCK_BY_PAGE` | `OnQueryCreditPledgeStkByPage`。 |
| `QueryCreditTargetStkByPage` | `CREDIT_TARGET_STOCK_BY_PAGE` | `OnQueryCreditTargetStkByPage`。 |
| `QueryCreditDebtToRepayTodayByPage` | `CREDIT_DEBT_TO_REPAY_TODAY_BY_PAGE` | `OnQueryCreditDebtToRepayTodayByPage`。 |
| `QueryCreditQuotaTransfer` | `CREDIT_QUOTA_TRANSFER_LOG` | `OnQueryCreditQuotaTransfer`。 |
| `QueryCreditMarginAvlTransfer` | `CREDIT_MARGIN_AVL_TRANSFER_LOG` | `OnQueryCreditMarginAvlTransfer`。 |
| 融资融券业务的 `InsertOrder` | `SECURITY_ORDER` + credit 标志 | `OnOrderEvent`、`OnTradeEvent`。 |
| 融资融券业务的 `CancelOrder` | `CANCEL_ORDER` | `OnOrderEvent`、`OnCancelOrderError`。 |
| 上述全部请求返回 0 | 同调用边界 `GetApiLastError`。 | `ERROR` / `NATIVE_CALLBACK`。 |
| 账户未开通能力 | capability gate。 | `CAPABILITY_UNAVAILABLE`，不调用 SDK。 |
| 所有异步结果 | 见完整 SPI 清单。 | 不依赖同步返回代表业务成功。 |

### 2.5 期权行权与组合策略（12）

| 官方方法 | canonical `operation_kind` / `query_kind` | 关联回调 |
| --- | --- | --- |
| `InsertOptionExerciseOrder` | `OPTION_EXERCISE_ORDER` | `OnOptionExerciseOrderEvent`。 |
| `InsertOptionCombinedOrder` | `OPTION_COMBINED_ORDER` | `OnOptionCombinedOrderEvent`。 |
| `QueryOptionCombinedUnfinishedOrders` | `OPTION_COMBINED_UNFINISHED_ORDERS` | `OnQueryOptionCombinedOrders`。 |
| `QueryOptionCombinedOrderByEMTID` | `OPTION_COMBINED_ORDER_BY_EMT_ID` | `OnQueryOptionCombinedOrders`。 |
| `QueryOptionCombinedOrders` | `OPTION_COMBINED_ORDERS` | `OnQueryOptionCombinedOrders`。 |
| `QueryOptionCombinedOrdersByPage` | `OPTION_COMBINED_ORDERS_BY_PAGE` | `OnQueryOptionCombinedOrdersByPage`。 |
| `QueryOptionCombinedTradesByEMTID` | `OPTION_COMBINED_TRADES_BY_EMT_ID` | `OnQueryOptionCombinedTrades`。 |
| `QueryOptionCombinedTrades` | `OPTION_COMBINED_TRADES` | `OnQueryOptionCombinedTrades`。 |
| `QueryOptionCombinedTradesByPage` | `OPTION_COMBINED_TRADES_BY_PAGE` | `OnQueryOptionCombinedTradesByPage`。 |
| `QueryOptionCombinedPosition` | `OPTION_COMBINED_POSITION` | `OnQueryOptionCombinedPosition`。 |
| `QueryOptionCombinedStrategyInfo` | `OPTION_COMBINED_STRATEGY_INFO` | `OnQueryOptionCombinedStrategyInfo`。 |
| `QueryOptionCombinedExecPosition` | `OPTION_COMBINED_EXEC_POSITION` | `OnQueryOptionCombinedExecPosition`。 |

### 2.6 账户终端、DMA 与准入查询（3）

| 官方方法 | canonical `query_kind` | 关联回调 |
| --- | --- | --- |
| `QueryEndpointId` | `ENDPOINT_ID` | `OnQueryEndpointId`。 |
| `QueryDMATickerAssignInfoByPage` | `DMA_TICKER_ASSIGN_INFO_BY_PAGE` | `OnQueryDMATickerAssignInfoByPage`。 |
| `QueryBlackWhiteListByPage` | `BLACK_WHITE_LIST_BY_PAGE` | `OnQueryBlackWhiteListByPage`。 |

## 3. `TraderSpi` 69 个回调覆盖

下表列出的每个 `callback_name` 都是 `NativeCallbackRecord` 的合法枚举值，并且必须在 Python binding bridge 中有明确 override。原生签名中的 `is_last`、`request_id`、分页序号、reference 和 `session_id` 必须完整保留；没有 `is_last` 的单次响应不可伪造该字段；回调处理均只复制并入队。

### 3.1 连接、普通订单与账户变动（16）

| 官方回调 | Gateway 规范化输出 | 处理要点 |
| --- | --- | --- |
| `OnConnected` | `SESSION_EVENT(CONNECTED)` + `NATIVE_CALLBACK` | 只通知，快速返回。 |
| `OnDisconnected` | `SESSION_EVENT(DISCONNECTED)` + Critical 流程 | 关闭新单，触发恢复。 |
| `OnError` | `NATIVE_ERROR_EVENT` + `NATIVE_CALLBACK` | 写结构化日志与告警策略。 |
| `OnOrderEvent` | `ORDER_EVENT` + `NATIVE_CALLBACK` | 订单状态事实；按 `order_emt_id` 归属。 |
| `OnTradeEvent` | `TRADE_EVENT` + `NATIVE_CALLBACK` | 成交事实；不可由订单状态替代。 |
| `OnCancelOrderError` | `CANCEL_ERROR_EVENT` + `NATIVE_CALLBACK` | 保留原单/撤单 ID 与错误。 |
| `OnFundTransfer` | `ACCOUNT_OPERATION_EVENT(FUND_TRANSFER)` + `NATIVE_CALLBACK` | 按 `serial_id` 关联。 |
| `OnCreditCashRepay` | `ACCOUNT_OPERATION_EVENT(CREDIT_CASH_REPAY)` + `NATIVE_CALLBACK` | 按 EMT ID 归属。 |
| `OnCreditCashRepaySpecifiedDebt` | `ACCOUNT_OPERATION_EVENT(CREDIT_CASH_REPAY_SPECIFIED_DEBT)` + `NATIVE_CALLBACK` | 按订单 ID 归属。 |
| `OnCreditExtendDebtDate` | `ACCOUNT_OPERATION_EVENT(CREDIT_EXTEND_DEBT_DATE)` + `NATIVE_CALLBACK` | 按 EMT ID 归属。 |
| `OnCreditQuotaTransfer` | `ACCOUNT_OPERATION_EVENT(CREDIT_QUOTA_TRANSFER)` + `NATIVE_CALLBACK` | 按 `serial_id` 归属。 |
| `OnCreditMarginAvlTransfer` | `ACCOUNT_OPERATION_EVENT(CREDIT_MARGIN_AVL_TRANSFER)` + `NATIVE_CALLBACK` | 按 `serial_id` 归属。 |
| `OnOptionExerciseOrderEvent` | `OPTION_EXERCISE_EVENT` + `NATIVE_CALLBACK` | 期权行权订单状态。 |
| `OnOptionCombinedOrderEvent` | `OPTION_COMBINED_ORDER_EVENT` + `NATIVE_CALLBACK` | 组合策略订单状态。 |
| `OnOptionCombinedTradeEvent` | `OPTION_COMBINED_TRADE_EVENT` + `NATIVE_CALLBACK` | 组合策略成交事实。 |
| `OnHoldingChangeEvent` | `HOLDING_CHANGE_EVENT` + `NATIVE_CALLBACK` | 账户持仓变动推送，不能当作行情。 |

> 本组共 16 个回调；覆盖计数以第 3 节所有表的 69 个唯一回调名为准。

### 3.2 普通账户查询回调（19）

`OnQueryOrder`、`OnQueryOrderByPage`、`OnQueryTrade`、`OnQueryTradeByPage`、`OnQueryPosition`、`OnQueryPositionByPage`、`OnQueryAsset`、`OnQueryFund`、`OnQueryFundTransfer`、`OnQueryOtherServerFund`、`OnQueryETF`、`OnQueryETFBasket`、`OnQueryETFByPage`、`OnQueryIPOInfoList`、`OnQueryIPOQuotaInfo`、`OnQueryOptionAuctionInfo`、`OnQueryEndpointId`、`OnQueryDMATickerAssignInfoByPage`、`OnQueryBlackWhiteListByPage`。

每个回调均输出 `ACCOUNT_QUERY_ITEM(callback_name=...)`；原生签名含 `is_last=true` 时输出 `ACCOUNT_QUERY_COMPLETE`。`OnQueryFund` 与 `OnQueryOtherServerFund` 是单次响应，必须在该回调完成对应 `request_id`，不得等待不存在的 `is_last`。

### 3.3 融资融券查询回调（25）

`OnQueryCreditCashRepayInfo`、`OnQueryCreditFundInfo`、`OnQueryCreditDebtInfo`、`OnQueryCreditDebtInfoByPage`、`OnQueryCreditTickerDebtInfo`、`OnQueryCreditAssetDebtInfo`、`OnQueryCreditTickerAssignInfo`、`OnQueryCreditTickerAssignInfoByPage`、`OnQueryCreditExcessStock`、`OnQueryMulCreditExcessStock`、`OnQueryCreditExtendDebtDateOrders`、`OnQueryCreditFundExtraInfo`、`OnQueryCreditPositionExtraInfo`、`OnQueryCreditPledgeStkRate`、`OnQueryCreditMarginRate`、`OnQueryCreditPositionFullRate`、`OnQueryCreditPledgeStkByPage`、`OnQueryCreditTargetStkByPage`、`OnQueryCreditDebtToRepayTodayByPage`、`OnQueryCreditQuotaTransfer`、`OnQueryCreditMarginAvlTransfer`、`OnQueryYesterdayAsset`、`OnQueryIssueInfoList`、`OnQuerySecurityInfo`、`OnQuerySecurityByPage`。

`OnQueryCreditFundInfo`、`OnQueryCreditAssetDebtInfo`、`OnQueryCreditExcessStock` 和 `OnQueryCreditFundExtraInfo` 是单次响应；其余本组回调按各自原生签名的 `is_last` 完成。`OnQueryMulCreditExcessStock` 虽带 `is_last`，但参数位置位于 `session_id` 之后，binding 适配不得假定统一参数顺序。

### 3.4 期权组合与港股通查询回调（9）

`OnQueryOptionCombinedOrders`、`OnQueryOptionCombinedOrdersByPage`、`OnQueryOptionCombinedTrades`、`OnQueryOptionCombinedTradesByPage`、`OnQueryOptionCombinedPosition`、`OnQueryOptionCombinedStrategyInfo`、`OnQueryOptionCombinedExecPosition`、`OnQueryHKConnectIndicativeExchangeRateInfo`、`OnQueryHkConnectSpreadTable`。

`OnQueryHKConnectIndicativeExchangeRateInfo` 是单次响应，无 `is_last`。

> 第 3.1（16）+ 第 3.2（19）+ 第 3.3（25）+ 第 3.4（9）= 69。每一个名称须由 P0 自动比对官方清单与 `fastspi` 注册表；差集非空即阻断发布。

## 4. Gateway 对外协议与权限

为覆盖全部交易/账户接口，同时保持无行情边界，策略数据面新增**版本化、强类型**操作，不开放 C++ 原生结构体或任意方法名反射：

| 协议消息 | 内容 | 权限 |
| --- | --- | --- |
| `ORDER_INTENT` | `SECURITY_ORDER`、`CANCEL_ORDER`，含常规/信用标志。 | `trade.order` / `trade.cancel`。 |
| `ACCOUNT_OPERATION_INTENT` | `FUND_TRANSFER`、四类信用操作、`OPTION_EXERCISE_ORDER`、`OPTION_COMBINED_ORDER`。 | 对应细粒度 `account.*` / `credit.*` / `option.*`。 |
| `ACCOUNT_QUERY_INTENT` | 第 2 节所有 `query_kind`；结果以 item/complete 异步回调返回。 | 对应只读 `account.query.*`。 |
| `ACCOUNT_QUERY_ITEM` | 原生查询 item、`callback_name`、`request_id`、分页元数据、原生签名提供时的 `is_last`。 | 仅原请求策略/经授权 Ops。 |
| `ACCOUNT_QUERY_COMPLETE` | 请求结果、最后 LSN、错误/完成状态。 | 同上。 |
| `ACCOUNT_OPERATION_EVENT` | 资金划拨、信用还款/展期/额度调拨等异步结果。 | 原请求策略与经授权账户订阅者。 |
| `NATIVE_CALLBACK` | 全量 SPI 的无损、版本化、脱敏副本。 | 仅具备 `native.callback.read` 的受控策略/Ops。 |

查询与账户操作是交易/账户服务，不是行情服务：不得请求实时行情、订阅行情或把 ETF、证券基础信息、港股通参考信息伪装为行情流；它们均为按需、受限、可审计的账户/交易参考查询。

## 5. 性能、顺序和失败语义

- 所有 69 个 SPI override 采用同一 `fastspi` 最小复制与 MPSC ring；禁止因为查询/账户回调“低频”就在 callback 内做日志、数据库、FlatBuffers、策略代码或企微 HTTP。
- `ORDER_EVENT`、`TRADE_EVENT`、`CANCEL_ERROR_EVENT`、期权订单/成交、资金/信用操作通知和 `OnHoldingChangeEvent` 是高优先级；查询 item 是低优先级。低优先级积压时仅限流该策略查询，不影响已持久化报撤单和高优先级回调。
- 每一种会返回 EMT ID / serial ID 的主动操作都按“准备记录 → `CALL_STARTED` → SDK 返回 ID → `NATIVE_ACCEPTED` → 专用回调/对账”建状态机。任何调用进入 `CALL_STARTED` 后均禁止自动重发，包括资金划拨、信用还款、期权行权和额度调拨。
- 非本实例/非本策略归属的账户回调不可丢弃：先以 `GetClientIDByEMTID`、账户和操作映射过滤；无法归属时写 `UNATTRIBUTED_NATIVE_EVENT`，进入账户级对账和告警，不猜测路由。
- 分页 response 的 `query_reference` 由 QueryScheduler 持久化；连续页只能由 scheduler 在前页完整完成后发起，不能在 SPI callback 内递归调用。

## 6. 验收与版本漂移门禁

1. P0 从目标 EMT Python binding 生成 `capability-report.json`，逐项输出本页 86 个方法和 69 个 callback 的 `binding_name`、签名、支持状态、账户权限、最小调用/回调探针结果，并记录 `GetApiVersion()`、binding 文件 SHA-256、Python ABI、操作系统和架构。
2. CI 从 `NativeMethodSpec` / `NativeCallbackSpec` 导出名称集合，与本页锁定清单逐项比较；缺失、重复、未知替代或未声明的新增项均失败。
3. 每个主动操作测试成功、SDK 返回 0、异常、断线、崩溃边界和对应 callback；每个查询测试空结果、错误、`is_last`、分页和迟到回调。
4. 每个 callback 测试最小复制、归属、审计、结构化 WARN+ 日志、AlertOutbox/企微链路和策略投递；所有流均可由 `correlation_id` 回放。
5. SDK 版本升级时先更新官方清单与 matrix，再运行 diff；只有新旧差异经审查、schema 新增字段向后兼容且 P0/P4 通过后才允许发布。
