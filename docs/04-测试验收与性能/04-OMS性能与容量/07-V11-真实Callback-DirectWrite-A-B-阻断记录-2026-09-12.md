# V1.1 真实 callback direct-write A/B 阻断记录（2026-09-12）

> 结论：**BLOCKED，未通过，也未产生 Direct candidate 性能结论。**
> 
> 范围：Linux EMT 模拟盘、当前已部署 revision `adc6acd` 的 TradeCore mmap/EMT 499-burst 控制路径。所有报告均为无业务载荷、无账户号、无订单号及无凭据证据。

## 1. 本次实际执行

在显式 armed 的模拟盘夹具、三 scope 独立进程、每 scope 一个 EMT API/session、固定 CPU 角色计划及 `MmapJournalProfileV1 / control` 下，实际释放一批 499 条信号。

| 检查项 | 结果 |
| --- | --- |
| C0 前 route / callback ownership 预登记 | 完成并 sealed |
| EMT `InsertOrder()` 同步受理 | `499/499` |
| C5 原生订单 callback ingress | `0/499` |
| C7 callback durable/downstream | `0/499` |
| callback gap | `0`；这不是成功，而是没有任何匹配 C5 样本 |
| Control 完整性 | 失败：`NO_MATCHED_CALLBACK_TO_C5` |
| 批内撤单调用受理 | `499/499` |
| callback 终态清理闭环 | 未能由订单 callback 证明，故本轮失败闭环 |
| 事后独立权威查询 | `QueryUnfinishedOrders` 完整，未完成委托 `0` |

`C4→C5` 是 EMT 异步外部区间。本次首个缺失边界正是 C5；因此不存在可归因于 control SPSC handoff、candidate direct writer、mmap commit 或 projecter 的 C5→C7 样本。

## 2. 原生交叉核验

为区分 TradeCore bridge 与柜台/SDK 投递，在同一模拟环境使用供应包官方示例同构的 `TraderApi` 子类、`subscribePublicTopic(2)` 与同一 client scope 做了一笔受控被动委托：

- `InsertOrder()` 与紧随其后的 `CancelOrder()` 均同步受理；
- 在 12 秒观察窗内，`onOrderEvent`、`onTradeEvent`、`onCancelOrderError` 的进入计数均为零；
- 紧接着独立 `QueryUnfinishedOrders` 完整结束，未完成委托为零，无需恢复撤单。

这项探针只保留方法进入计数，不记录回调载荷或身份数据。它与 Control 在 C5 的结果一致，足以排除“仅 TradeCore callback normalizer 或 control/candidate 交接造成 C5 缺失”的解释；但它**不**能替代 EMT 对当日模拟环境订单回报投递状态的权威说明。

## 3. A/B 判定

Direct candidate 未启动。该候选只改变 C5 之后的 scope-local handoff；在 Control 没有一个 C5 到达时再次发送 499 条不能比较 C5→C7，且会无谓扩大模拟盘副作用。因此：

```text
Control：实际执行，C5 缺失，FAIL
Direct：未执行（避免不可比较的重复副作用）
三组交错 A/B：未开始
默认路径：保持 control，candidate 保持关闭
```

不得把历史 499 基线、注入 callback 的合同测试、`InsertOrder()` 同步返回，或查询归零，写成此次 callback direct-write A/B 的通过证据。

## 4. 已采取的安全动作

1. Control 结束后执行独立查询恢复；权威未完成计数为零。
2. 原生交叉探针完成后再次执行独立查询恢复；计数仍为零。
3. 原始模拟盘夹具以及诊断副本均已设置为 `armed=false`。
4. 后续代码修订补上了通用 armed gate：除零订单 session preflight 外，所有能够进入 `InsertOrder` 的 fixture 路径（包括没有 tight-plan 指纹的 legacy 路径）都必须先验证为显式 armed simulated fixture。该栅栏不改变本次已完成的运行结论。

## 5. 恢复 A/B 的唯一入口条件

在 EMT 确认/修复模拟环境订单回调投递后，按以下顺序重新开始，不能跳过：

1. 在每个候选 client scope 运行同构原生回调运输探针，证明受理后的 `onOrderEvent` 至少一次进入；若失败，停止且不发 499-burst。
2. 用相同 SDK、主机、CPU plan、scope 数、fixture、callback pipeline 和已校准时钟运行一轮 Control；要求 `499/499` C5、C7、完整配对、零 gap、清理 terminal 闭合和 query-only 为零。
3. 在相同条件运行 Direct candidate；要求完全相同的完整性与恢复证据。
4. 完成至少三组交错 Control/Direct，并逐轮比较 C5 子段、`C5→C7`、batch wall-clock、frame/replay/watermark/snapshot 等价性及回退演练。

只有全部完成时，才可讨论是否改变默认 callback 路径；仅获得 EMT 对回调机制的文字答复也不足以替代上述实测。
