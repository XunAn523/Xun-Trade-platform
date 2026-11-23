# Kungfu OMS / EMS / 路由参考与 TradeCore 落地约束

> 状态：架构参考已核验（2026-09-05）。本文不是第二份实施基线，也不引入 Kungfu 源码、二进制或运行时依赖；TradeCore 的唯一实施与验收基线仍为 [OMS-EMS-Adapter-PaperBroker模块化实施方案](./04-OMS-EMS-Adapter-PaperBroker模块化实施方案.md)。

## 1. 参考范围与结论

本次核验的参考仓库是 `参考/xlquant_broker/kungfu-systems/kungfu`。其没有把职责命名为两个独立的 “OMS 服务 / EMS 服务”，而是由下面四个协作部件完成等价职责：

| Kungfu 部件 | 职责 | TradeCore 对应边界 |
| --- | --- | --- |
| `strategy::LiveContext` | 创建 `OrderInput`，确定目标交易账户并写入目标 TD channel | Consumer → OMS 准入后生成 `ExecutionInstruction` |
| `broker::TraderVendor` + `OrderService` | 订阅目标账户输入、做就绪/风险来源判断、调度报撤单 | EMS 的每账户 `ExecutionScheduler`、`AccountExecutionRouter` 与会话监督器 |
| `TraderEMT` | EMT 字段转换、`InsertOrder` / `CancelOrder`、原生 ID 双向映射、SPI 回报归一化 | `EmtControlledExecutionAdapter`、平台执行会话与 EMT mutation / event ingress |
| `service::Ledger` + `Bookkeeper` | 旁路订阅订单、成交、资产、持仓，维护账本并向订阅者派生视图 | OMS 订单状态/保留、对账、异步 read model 与快照服务 |

Kungfu 最值得采用的结构结论是：**账户执行是固定单元；订单命令以账户为目标写入；原生回报依靠稳定订单关联直接回写原始消费者；账本是订阅者而不是报单临界路径的一跳。**

TradeCore 已有比参考实现更严格且必须保留的保证：Journal-first 副作用证据、签名 scope ownership、静态 route plan、可验证的 callback-gap fail-closed、完整 Broker 快照恢复裁决，以及每账户有界新单/撤单队列。因此不能把 Kungfu 的实现直接复制进来。

## 2. Kungfu 的实际信息流

### 2.1 报单

`LiveContext::insert_order` 先按 `(source, account)` 取得 TD location，检查该账户就绪后把 `OrderInput` 写入以该账户为 destination 的 journal；订单 ID 来自该 writer 的 frame ID。可见 `framework/core/src/libkungfu/wingchun/strategy/live.cpp` 的 `insert_order`。

TD 侧 `TraderVendor::react` 订阅 `OrderInput`。`OrderService::on_order_input` 做风险来源、批量/大宗模式判断后调用具体柜台适配器的 `insert_order`；该路由不是按标的临时选柜台，而是由 command 的目标 TD location 决定。可见 `framework/core/src/libkungfu/wingchun/broker/tradervendor.cpp` 与 `broker/order.cpp`。

EMT 适配器 `TraderEMT::insert_order` 在 SDK 返回非零 `order_emt_id` 后建立 `kungfu_order_id ↔ emt_order_id` 双向映射，并把初始 `Order` 写回请求事件的 source writer。撤单用同一映射取得精确 `order_emt_id`，未知映射直接拒绝。可见 `extensions/emt/src/cpp/trader_emt.cpp`。

```text
Strategy / Consumer
  └─ OrderInput (destination = account TD)
       └─ account execution lane
            └─ adapter InsertOrder
                 └─ order identity binding
                      └─ initial order result -> original consumer route
```

### 2.2 回报、成交与账本

`OnOrderEvent` 和 `OnTradeEvent` 先用 `emt_order_id` 找回 Kungfu order ID；未知外部订单或成交不猜测归属。成交额外以 `exec_id` 去重。回报写回订单保存的 destination writer，因此回报不是经一个中心 UI 或账本服务中转后再返回策略。

`TraderWriterHook` 在 Order / Trade frame 关闭时更新本地订单状态服务；`Ledger` 独立订阅 `OrderInput`、`Order`、`Trade`，更新账户/策略 book、订单统计和资产/持仓派生视图。这说明账本观察事实，但不挡在 `InsertOrder` 前。

```text
EMT SPI callback
  └─ (emt_order_id, client/order identity) -> exact local order binding
       └─ original consumer route receives normalized Order / Trade
            └─ Ledger / projection subscribe asynchronously
```

### 2.3 传输与路由基元

Kungfu 的 `yijinjing` journal 为每个 `(source location, destination)` 提供 writer / reader；writer 以内存映射 page 写固定布局 frame，reader 对已加入 channel 合并读取。frame 自带 `source`、`dest`、`msg_type`、生成时间和 frame ID。`TraderVendor::on_write_to` 为目标 destination 建立 writer；`Ledger::inspect_channel` 根据 channel 决定订阅关系。

该设计把“业务路由键”和“本地 transport destination”统一成 location UID，获得极短的本机路径；代价是路由拓扑、journal 与进程内状态强耦合，且其本身不提供 TradeCore 所需的签名账户归属、Redis 原子副作用证据和 callback gap 裁决。

## 3. TradeCore 的采用方式

TradeCore 采用相同的**逻辑拓扑**，但保留当前更强的持久化与安全边界：

| 参考原则 | TradeCore 已有实现 | 固定落地规则 |
| --- | --- | --- |
| 账户是执行单元 | `ems/routing.py` 的 `AccountExecutionBinding` / `AccountExecutionRouter` | 新单、撤单、回报只能解析到一个静态 account lane；账户、venue、adapter instance 或 client scope 任一不符即拒绝。 |
| 每账户有界单写者/优先队列 | `ems/scheduler.py` 的 `ExecutionScheduler` | 新单与撤单隔离；撤单优先；不以全局锁或无界队列替代。 |
| 发送前具备稳定订单关联 | `oms/order_book.py`、`emt_gateway/tradecore_redis_journal.py` | `OMS ID → execution ID → client ref → EMT ID` 必须在可恢复 Journal 事实中闭合；禁止按价格、时间或标的猜测。 |
| 回报直达原消费者 | `adapters/emt/event_ingress.py` 与 Consumer event stream | 回调以 `VenueOrderIdentity` 和持久 route directory 归属到原 consumer；OMS/projection 订阅同一事实，不能成为回传中继。 |
| 账本旁路 | `oms/order_book.py`、异步 archive / PostgreSQL projection | 准入所需的 OMS 原子事实可在 C1 完成；PG、SSD archive、快照刷新和通知绝不增加 C1→C4 或 C5→C7 同步等待。 |
| 柜台适配器只处理柜台语义 | `EmtControlledExecutionAdapter`、`emt_gateway/tradecore_execution.py` | EMT SDK 对象、会话和字段不得越过 `BrokerAdapter` 边界进入 OMS。 |

## 4. 明确不照搬的部分

1. **不把 mmap journal 替换为 Redis Journal。** Kungfu 的 mmap frame 同时承担本机消息 transport 与历史 journal；TradeCore 的 Redis 原子 stream/index 承担副作用前恢复证据。删除或异步化这一证据会破坏进程崩溃后的 `CALL_STARTED` 裁决。
2. **不使用动态 channel 加入决定交易权限。** Kungfu 的 channel 可以运行时建立；TradeCore 的路由必须来自已签名的静态 `AccountExecutionRoutePlan` 和 ownership fence，不能由业务消息改变。
3. **不把适配器内存映射作为唯一事实。** Kungfu 的 `emt_to_kungfu_order_id_` 是进程内关联；TradeCore 必须同时保持可恢复的 route directory 与 callback journal。
4. **不在 Python SPI 回调执行 writer / journal I/O。** Kungfu C++ writer 可以在适配器回调中写 frame；TradeCore EMT Python binding 的回调先经原生 `fastspi v2` / 有界 ingress，再由 callback worker 进行 Redis 与 OMS 工作。
5. **不将 Ledger 的派生 book 视为柜台权威。** TradeCore 崩溃恢复仍以完整 Broker/执行端快照裁决，OMS/PG/SSD 仅按定档职责参与。

## 5. 后续性能改造的具体约束

后续针对 C0→C4 与 C5→C7 的优化，必须遵守以下顺序：

1. 将每笔命令在 C1 之后解析出的 `AccountExecutionLane`、EMT client reference 和回传 consumer route 固化为紧凑、不可变的 route handle；C2/C3 不再重新做策略、账户或 profile 查找。
2. 保持“账户 lane → 原生调用”的单向数据流；所有 UI、读模型、对账、通知和非关键 telemetry 均订阅事实，禁止回插到该 lane。
3. callback worker 以已经固定的 `VenueOrderIdentity` / route directory 回传，不通过账户查询、全局订单扫描或近似匹配寻找 consumer。
4. 若要引入更短的本机 transport，只能作为 Redis durable commit **之后**的内存 dispatch 加速层；它必须有容量上限、溢出证据和 fail-closed 处理，且不能取代 Redis 事实。
5. 任何并发扩大前，先保持每个 `(account, venue, adapter_client_scope)` 一个会话和一个逻辑 lane；并发仅在 EMT 已证明允许的调用边界内增加，撤单因果顺序不可打破。

这些规则把 Kungfu 的低延迟路由优点转化为 TradeCore 的工程约束，同时不牺牲现有模拟盘验收已证明的审计、隔离、恢复和回调完整性。
