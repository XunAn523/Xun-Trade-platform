# EMT 模拟盘接口阻断统计

> 状态：**当前实测阻断清单**（更新至 2026-09-15）。本文件只统计已在 EMT 模拟盘或目标 Linux binding 中得到“未通过、拒绝或不可调用”结论的项目；不把未配置受控 fixture、没有发生相应业务事件、或已在当前 release scope 禁用的接口伪装为失败。所有结果均为脱敏 evidence，不包含账户、标的、订单、资金、地址、凭据或原始回调 payload。

## 1. 统计边界

当前发布 scope 为 `emt-ordinary-credit-v1`：普通证券、资金、信用、DMA 与参考查询启用；期权 domain 在 SDK 调用前拒绝。普通/信用只读候选合计 46 个，其中 41 个已取得完整终态，5 个存在真实模拟盘阻断。

```text
当前启用 scope 的已测只读候选：46
  ├─ 完整终态：41
  └─ 未通过：5

Linux Python binding 缺失：2 个主动 API
  └─ 对应的 2 个回调能力也不能真实触发

历史已测、但当前 scope 禁用或条件不满足：6 个阻断项
```

查询主动调用返回 `0` 仅表示 SDK 已受理请求；它不是结果、空结果或查询完成。对于携带 `(request_id, is_last, session_id)` 的查询回调，TradeCore 仅接受匹配 `request_id` 且最终 `is_last=true` 的 response sequence 作为完成证据。

## 2. 当前普通/信用 scope：真实未通过接口

| API | profile | 实测事实 | 分类 | 对发布的影响 |
| --- | --- | --- | --- | --- |
| `QueryIPOInfoList` | ordinary | 原生调用返回 `0`；30 秒内没有 callback，且无 `OnError`。 | `native_accepted_no_callback` | IPO 参考查询不具备可验证完成语义；不能作为已验收能力。 |
| `QueryYesterdayAsset` | ordinary | 原生调用返回 `0`；30 秒内没有 callback，且无 `OnError`。 | `native_accepted_no_callback` | 昨日资产无法用于日切/资产核对的权威输入。 |
| `QueryCreditPositionFullRate` | credit | 原生调用返回 `0`；收到一条匹配 response，`error_id=0`、`is_last=false`；后续 30 秒未收到终态。 | `native_accepted_missing_is_last_true` | 融券头寸全额占用费率不能标为完成；普通/信用 scope 不能进入 `READY`。 |
| `QueryCreditExcessStock` | credit | 原生阶段返回 `230113`，没有 callback。 | `unsupported_simulated_venue` | 当前模拟柜台不提供该信用余券查询。 |
| `QueryMulCreditExcessStock` | credit | 原生阶段返回 `230113`，没有 callback。 | `unsupported_simulated_venue` | 当前模拟柜台不提供该多市场信用余券查询。 |

前三项已在 revision `6652cb7` 使用单会话、单请求、30 秒、payload-free 原生终态 probe 复验。probe 已验证 Linux Python binding 的五参数 callback shape，并排除了 parser、callback ring、worker 延迟与 probe wrapper 析构对结论的影响。

## 3. Linux Python binding：不可调用能力

官方 C/C++ API 包含下列两地分仓保证金调拨接口，但当前目标 Linux Python binding 不导出主动调用入口。TradeCore 固定在 SDK 调用前返回 `CAPABILITY_UNAVAILABLE/unsupported_binding`；不得以 `ctypes` 猜测 ABI、伪造 callback 或用其他调拨接口冒充成功。

| API / SPI 对 | 官方用途 | 当前状态 | 影响 |
| --- | --- | --- | --- |
| `CreditMarginAvlTransfer` / `OnCreditMarginAvlTransfer` | 发起并接收两地分仓保证金调拨结果。 | 主动 API 不可调用，回调不能由真实请求触发。 | 不支持保证金调拨操作闭环。 |
| `QueryCreditMarginAvlTransfer` / `OnQueryCreditMarginAvlTransfer` | 按 `serial_id` 查询保证金调拨流水并获得查询终态。 | 查询 API 不可调用，回调不能由真实请求触发。 | 不支持保证金调拨后的权威核对与恢复对账。 |

这不影响已通过的 `CreditQuotaTransfer` 额度调拨闭环；两者是不同业务：前者为保证金调拨，后者为融券额度调拨。

## 4. 历史真实测试、但不属于当前 ordinary/credit 启用范围的阻断

| API | profile / 条件 | 实测事实 | 当前处理 |
| --- | --- | --- | --- |
| `QueryOptionCombinedOrdersByPage` | option | 原生调用返回 `6077`，无终态 callback。 | 当前模拟柜台不支持；期权 domain 已禁用。 |
| `QueryOptionCombinedTradesByPage` | option | 原生调用返回 `6077`，无终态 callback。 | 当前模拟柜台不支持；期权 domain 已禁用。 |
| `QueryYesterdayAsset` | option | 两次独立 probe 均返回 `6077`，无 callback。 | 当前模拟柜台不支持；期权 domain 已禁用。 |
| `QueryCreditExtendDebtDateOrders` | credit | 原生调用返回 `230113`，无 callback。 | 当前模拟盘没有可用于展期查询/对账的业务能力。 |
| `CreditExtendDebtDate` | credit | 当前无可验证的有效展期债务，不能构造正确请求。 | 不执行，不用过期债务、虚构 EMT ID 或重试制造样本。 |
| `CreditCashRepaySpecifiedDebt` | credit | 历史受控调用取得 native 操作 ID 后未收到专属 callback，后续只读核对未形成完整闭环。 | 保留为 capability blocker；不自动重试。 |

## 5. 热路径回调验收状态（2026-09-15）

本节与“查询阻断”分开记录：交易回调必须先由对应业务动作真实触发；没有成交事实时，不能把“没有 `OnTradeEvent`”误写成 SDK 或 SPI 失败。

| 热路径 API / SPI | 当前证据 | 结论 | 边界 |
| --- | --- | --- | --- |
| `InsertOrder` → `OnOrderEvent` | 当前 Linux 分片基线已取得真实 499 笔 native accepted、callback `C7` 与零 gap；受控单笔也取得 native accepted。 | **已通过** | 这是委托受理/订单回报闭环，不证明成交。 |
| `CancelOrder` | 当前 Linux 分片基线与受控单笔均取得真实 native accepted 和终态查询核对。 | **已通过** | 只证明本系统已归属订单的撤单；不授权外部订单撤单。 |
| `OnCancelOrderError` | revision `a0a2d8d` 的一次模拟盘受控演练中：首笔报单、首次撤单、权威终态查询、终态后第二次撤单均成功；收到同时匹配原订单原生 ID 与第二次撤单原生 ID 的 callback。 | **已通过** | 是单笔真实兼容性/路由证据，不是吞吐或延时 SLA。迟到 callback 不能满足该检查。 |
| `OnTradeEvent` | revision `8c58569` 修正了沪市受控成交 fixture 的非挂单价格类型（`4`）；修正后单笔 100 股真实委托取得 `InsertOrder` native accepted 与订单 callback，但 8 秒观察窗内仍没有成交 callback。 | **待验收** | 本次已排除价格类型配置错误，但仍没有形成成交事实；结果不能升级为通过，也不作为接口异常。需要 EMT 模拟柜台实际撮合的、可清理的最小成交候选后重测。 |
| `CreditSellStockRepayDebtInterestFee` | Linux Python binding 可直接调用。只读预检已取得同账户的实时最小候选；revision `3930904` 的单笔、预留 `order_client_id`、Journal-first compatibility probe 取得同步 `0` 与 `GetApiLastError.error_id=230113`，未得到 native order id 或 callback。 | **`CAPABILITY_UNAVAILABLE / unsupported_simulated_venue`** | 此为确定本地拒绝、无外部订单副作用；不自动重试。当前 EMT native execution adapter 仍没有该操作的正式发射器，故该 probe 也不能替代产品路径验收。 |

`OnCancelOrderError` 的通过不改变本文件第 2 节的五项查询阻断，也不把整体发布结论升级为 `READY`。`OnTradeEvent` 仍须由真实成交触发；信用卖券还款须由 EMT 提供支持该业务的模拟柜台或正式环境，并先完成正式 adapter 的 Journal-first 发射实现，才可形成可验证的真实回调闭环。

## 6. 不计为失败的项目

下列情况不进入本统计，避免扩大“异常接口”范围：

1. 已通过当前验收的普通资金划拨、信用额度调拨、普通/信用订单与撤单、以及 `TC-EMT-499` callback 闭环。
2. 透明 RST 演练中未出现 `OnDisconnected`：该 SDK revision 已按“传输自动重连 + 恢复 `OnConnected`”的限定语义完成验收；它不等同完整断线风暴覆盖，但不是当前失败接口。
3. 因无成交、无有效债务、无组合策略或未获独立 fixture 而未触发的事件。这些属于 `pending`/条件不足，不得写成“接口异常”。
4. 历史上曾缺少终态、但已经在更新后的 matrix 中通过的项目；旧失败 evidence 保留审计价值，但不覆盖当前结论。

## 7. 关闭条件

| 阻断类型 | 关闭所需证据 |
| --- | --- |
| 查询终态缺失 | EMT 修复柜台/SDK，或供应商提供版本化、可验证且等价于终态的语义；固定新 SDK hash 后，同一 probe 连续 3 轮均须收到匹配 `request_id` 的最终 `is_last=true`，零 callback gap。 |
| `230113` / `6077` | EMT 提供该模拟盘业务权限、有效业务对象或明确支持版本；先低频单请求 probe 成功，再按所属 domain 完成独立功能和清理验收。 |
| Python binding 缺失 | EMT 提供 ABI 已验证的 Linux CPython 3.10 binding；先重跑 ABI catalog/SPI signature gate，再做最小可逆 mutation、专属 callback、按 serial 查询终态、恢复/清理与 499 非干扰验收。 |
| 无有效业务对象 | EMT 提供当前交易日有效且经授权的债务/策略/组合对象；不允许由 TradeCore 伪造对象或用历史失效 ID 代替。 |
| 无模拟撮合事实 | EMT 提供会实际撮合受控最小订单的模拟环境或批准的真实受控成交窗口；重跑时必须取得本进程 order id、`OnTradeEvent`、精确成交查询与测量窗外清理证据。 |

在任一关闭条件未满足前，相关 capability 继续 fail-closed。普通/信用 scope 的发布状态保持 `DISARMED_NOT_READY`；不自动交易、自动恢复、自动撤单或自动重报。

## 8. 依据

- [普通与信用账户发布范围与验收定档](./14-普通与信用账户发布范围与验收定档.md)
- [EMT 全交易接口 OMS / EMS 实施与验收定档](./13-EMT全交易接口OMS-EMS实施与验收定档.md)
- [全接口压测建设方案](../04-测试验收与性能/03-EMT接口性能/01-全接口压测建设方案.md)
- [EMT 官方 2.27 API 文档](https://emt.eastmoneysec.com/api/help/emt__api_8h.html)
