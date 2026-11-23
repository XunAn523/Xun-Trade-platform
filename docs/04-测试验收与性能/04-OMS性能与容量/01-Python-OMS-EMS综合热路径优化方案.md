# Python TradeCore OMS/EMS 综合热路径优化方案

> 状态：O0 已在现行 mmap benchmark 落地；O1 已完成 EMT 模拟盘 499 笔交错 A/B 验收并设为默认；O2--O4 待实施。
>
> 适用范围：Linux / CPython 3.10 的 TradeCore EMT-only 执行适配器；当前已证明可用的四个独立 scope/session/process 分片；499 笔同时到达的 EMT 模拟盘验收模型。
>
> 不适用范围：PaperBroker、策略收益逻辑、交易所/柜台网络延时、柜台撮合延时，以及将同一 EMT session 改为多 caller 并发调用。

## 1. 结论与设计目标

TradeCore 应综合借鉴以下三类设计，而不迁入任一完整框架：

| 参考 | 应借鉴的能力 | 不迁入的部分 |
| --- | --- | --- |
| Kungfu / Yijinjing | scope-local Journal、提交后发布、订单 ID 双向关联、回放恢复、账本旁路订阅 | C++ runtime、全套 Ledger/UI、将 Python 热路径替换为 Kungfu TD |
| WonderTrader | 策略 EMS 与原始报单通道隔离、目标仓位/拆单/执行算法分层、按执行器路由 | 在原始下单热路径中进行目标仓位计算、频率历史扫描、同步日志和多层锁 |
| VeighNa / vn.py | 统一的订单/成交/资金/持仓只读查询模型、网关归一化事件 | 全局单 `Queue`、单线程事件分发、将 OMS 作为发单前同步中继 |

目标架构的原则是：**发单与 callback 热路径只完成副作用所需的最小、确定性、可恢复操作；OMS、EMS、查询、审计扩展和运营功能都消费已提交事实，不能反向阻塞柜台调用。**

```text
策略 / 上游信号
  → C0 ingress：动态订单字段确认、准入、scope 决策
  → scope-local native lane
  → C2 PreDispatch mmap Journal commit
  → C3/C4 EMT InsertOrder
  → EMT native callback
  → C5/C6 scope-local callback mmap Journal commit
  → C7 最小状态投影及下游交付
  └→ 异步 OMS projector / 查询快照 / EMS 观测 / 审计归档 / 告警
```

`C4A→C5` 是 SDK、网络和 EMT 柜台的异步等待，不计入项目内部性能预算；但必须继续采集，避免将外部问题错判为 TradeCore 回归。

## 2. 当前事实与不可突破边界

### 2.1 当前已验证的性能事实

最近一次同配置三轮 499 笔全量模拟盘 burst 的逐轮分位数中位数如下。每轮均为 499/499 native accepted、499/499 C7、无 callback gap、清理后无 in-flight；数据不包含账户、标的、价格、订单号或凭据。

| 区间 | P50 ms | P95 ms | P99 ms | P100 ms |
| --- | ---: | ---: | ---: | ---: |
| 项目内部提交 `C0→C4A` | 7.362390 | 13.378196 | 13.912184 | 14.266568 |
| 项目内部回调 `C5→C7` | 4.448059 | 11.180975 | 11.393270 | 11.457405 |
| EMT 外部等待 `C4A→C5` | 30.259017 | 47.647410 | 48.604716 | 48.815718 |
| 完整直接链路 `C0→C7` | 46.312806 | 64.212980 | 65.775952 | 65.877482 |

当前最需要处理的项目内部候选段并非 mmap commit：

| 子阶段 | P95 ms | 设计含义 |
| --- | ---: | --- |
| `C0A→C0B` native scheduler queue | 11.881811 | 每个 session 的单 native lane 在约 124/125 笔分片 burst 中串行排队 |
| `C1` route preparation | 0.052919 | 已较小，仍可减少单笔 Python 波动 |
| pre-dispatch mmap commit | 0.007931 | 已是微秒级，不应为追逐小数点而牺牲恢复语义 |
| `C3→C4` SDK 主动调用 | 0.006940 | 本地同步调用本身不是 499 burst 的主瓶颈 |
| `C5→C5D` native callback 到 Python drain | 7.148060 | 需要精细分段归因 |
| `C5A→C5B` callback target worker queue | 5.310733 | 可通过减少无必要交接进行 A/B 验证 |
| callback mmap commit | 0.012128 | 已是微秒级，不作为优先优化对象 |

这些分位数来自逐轮统计；不同列的分位数不能直接相减或相加推导单笔子阶段耗时。

### 2.2 必须保留的边界

1. **Journal-first：** PreDispatch frame 完整提交前不得调用 EMT；frame 必须先写完内容与校验，最后写入 commit word。不得以同步 `fsync`、同步 `msync` 或同步外部存储伪造更强恢复承诺。
2. **单会话串行：** 每个 `(account, venue, client scope)` 只能由一个 native caller 调用 EMT SDK。当前没有可接受证据支持同一 session 的多 caller 并发。
3. **独立分片：** 一个 session 对应一个 Python 进程、一个 native lane、一个 mmap shard 和一条 callback 生命周期。当前经过证明的容量为四个 scope；第五个 scope 的零副作用 readiness 未闭合，不能启用或假设存在。
4. **fail-closed：** route identity 冲突、Journal 容量耗尽、callback gap、callback 归属未知或 adapter epoch 不匹配，必须停止新单，而非退回无 Journal 或无映射路径。
5. **测量诚实：** 任何订单特有的映射、校验和字段填充都必须计入 C0 之后的分段；仅 session 激活时就已知的静态表、固定 frame 模板和 route capability 才能在 C0 前预构建。

## 3. 最终分层架构

### 3.1 热路径：Execution Adapter

热路径仅拥有发送命令和把权威回报变为 durable fact 的职责：

```text
Ingress command
  → immutable PreparedEmtSubmit
  → bounded per-scope scheduler
  → PreDispatch frame
  → native EMT request construction
  → EMT InsertOrder / CancelOrder
  → synchronous receipt and external-ID binding

Native callback
  → client-reference / external-ID route lookup
  → fixed callback envelope
  → callback frame commit
  → minimal state transition + C7 delivery
```

热路径禁止包含：完整 OMS 查询、账户/持仓轮询、策略算法、JSON、字符串格式化日志、数据库、Redis、网络 RPC、SSD 强制刷盘、告警、报表和 UI 通知。

### 3.2 OMS：事实投影与恢复，不参与发单决策

新增或收敛为 scope-local `OrderStateProjector`，以 Journal 的事实流为唯一输入：

- 主关联：`execution_id ↔ canonical client reference ↔ external_order_id`；
- callback 到达时优先用提交前已登记的 client reference 找回归属；SDK 主动调用返回后补充 external ID 索引；不得假设 callback 一定晚于 `InsertOrder` 返回；
- 每个事实有 schema version、序号、状态版本和来源标识；状态只能单调推进；
- 成交以柜台成交唯一键去重，订单回报以状态版本/单调序列防重；
- projector 可以从 mmap Journal 重放；启动、断线重连或崩溃恢复后，再用 EMT 权威查询对账；
- 对查询侧发布不可变快照引用。查询、监控和 UI 不得持有 native-lane 锁或 callback writer 锁。

该层借鉴 Kungfu 的订单状态回放与 Ledger 旁路思想，以及 vn.py 的统一只读状态接口；但它不成为提交前的同步依赖。

### 3.3 EMS：在订单意图形成之前工作

EMS 负责策略层的复杂性，输出标准 `ExecutionInstruction`，但不直接调用 SDK：

```text
策略目标仓位 / 风险预算 / TWAP / VWAP / 最小冲击
  → EMS 生成或撤销 ExecutionInstruction
  → TradeCore Execution Adapter
```

EMS 可拥有策略路由、目标仓位净额、拆单、执行节奏、频率策略和撤单策略。它必须在 C0 前形成订单意图；最终的 C0 准入仍由 Execution Adapter 进行，以保证 scope ownership、Journal-first 和 fail-closed 边界不被绕过。

## 4. 分阶段优化方案

### O0：补全测量，不改变交易行为

目的：先证明 callback 的 7 ms / 5 ms 候选段分别是 SDK 回调线程、Python/GIL 调度、队列交接、route 查表还是 mmap 写入所致。

新增 payload-free 时间点：

```text
C5      EMT native callback 到达
C5A     client-reference/external-ID route lookup 完成
C5B     callback envelope 已写入 scope-local ingress
C5C     callback mmap frame 打开前
C5D     callback mmap commit 后
C5E     projector 开始处理
C6      归一化 outcome durable
C7      最小下游交付完成
```

同时每个 scope 采集：队列深度、高水位、唤醒次数、GC、minor/major page fault、CPU migration、voluntary/nonvoluntary context switch。所有诊断写入内存缓冲，测量窗口结束后一次性输出；不得逐订单同步文件写入。

**通过条件：** 499 笔每条均有完整配对时间；没有将旧 Redis 标签用于 mmap profile；每个 P99/P100 异常能归属到一个本地子段或标记为外部等待。

### O1：PreparedEmtSubmit——降低单笔常量开销

借鉴 Kungfu 的固定 `OrderInput → TD` 映射，但保持 TradeCore 的 Journal-first 边界。

在 session 激活时缓存静态数据：scope identity、venue capability、市场/业务类型转换表、固定 ASCII 字段、mmap frame 模板和 adapter epoch。C0 后仅填入订单特有字段并创建不可变 `PreparedEmtSubmit`：

- 动态字段：execution identity、方向、数量、价格、价格类型、过期时间和 client reference；
- 已验证的 scope/adapter identity 摘要；
- 已绑定的 scheduler、Journal writer 与 native dispatch callable；
- 不包含 SDK 原生对象、可变业务对象或秘密材料。

native lane 中只做 epoch/identity 防御校验、PreDispatch commit、原生请求对象构造、SDK 调用和返回后的 external-ID 发布。

**安全测试：** adapter 重启后旧 handle、跨 scope handle、instruction identity 不一致、过期 handle 都必须在 Journal/SDK 之前失败；正常动态路径与 prepared 路径结果必须完全一致。

**性能判断：** 该阶段主要改善单笔和低并发波动。现有 499 P95 的主因是 native-lane 排队；不能将 O1 的完成表述为解决了 11.88 ms 排队。

**实现与验收状态（2026-09-08）：** `PreparedEmtSubmit` 在 adapter 已激活、C0 之前生成，只包含已映射的领域请求、canonical client reference、adapter identity 和 activation epoch；不创建 EMT SDK 原生对象、不调用 mutation port、不写 Journal。native lane 仍先执行 PreDispatch mmap commit，再构造原生请求并调用 SDK。旧 descriptor、跨 activation descriptor 和跨 adapter descriptor 均在 Journal/SDK 前拒绝。代码通过 460 项本地测试（21 项按环境跳过）和变更范围 lint。

同一 Linux、四 scope/process、相同 CPU affinity、`threading.Event / 8` callback pipeline、GC 测量窗口与受控模拟 fixture 下，完成三组交错 dynamic control / O1 candidate 499-burst；六轮均为 `499/499` native accepted、`499/499` C7、`499/499` 完整 C5→C7 时间对、`0` callback gap、`499/499` 清理撤单获接受，terminal inflight 为 `0`。下表为三个每轮 nearest-rank 分位数的中位数（ms），不池化：

| 项目内路径 | dynamic control P50 / P95 / P99 / P100 | O1 prebuilt P50 / P95 / P99 / P100 |
| --- | ---: | ---: |
| `C1→C2` adapter / pre-native path | 0.054613 / 0.118981 / 0.435233 / 0.738614 | 0.012600 / 0.015533 / 0.059351 / 0.130729 |
| `C0A→C0B` native scheduler queue | 6.780485 / 12.571612 / 13.063200 / 13.386392 | 3.654316 / 5.760045 / 5.942272 / 6.043508 |
| `C0→C4A` project-internal submit | 7.881750 / 13.958044 / 14.560551 / 14.850241 | 4.582493 / 7.124733 / 7.431261 / 7.519125 |
| `C5→C7` project-internal callback | 6.961867 / 10.191774 / 14.280437 / 14.560314 | 6.066894 / 9.469050 / 9.598447 / 9.683805 |

这证明 O1 降低了 native lane 内每笔动态映射常量成本，并在 499 条全接纳中按每 scope 的串行队列深度放大为提交尾延时改善；它不改变 session 数，也不构成 EMT 外部延时优化。`C4→C5` 的三轮 P95 在该窗口由 `46.421062 ms` 变为 `53.999419 ms`，因此 `C0→C7` 和 callback batch wall-clock 不作为 O1 改善结论。prebuilt 路径现为默认；可使用 `--dynamic-submit-mapping` 进行同修订 A/B 或故障回退，`--prebuild-submit-mapping` 保留为显式选择。

**默认化补充验收（不计入上表，2026-09-08）：** 默认开关的后续单轮同口径 499-burst 取得 `499/499` native accepted、`499/499` C7 与 `499/499` C5→C7 时间对，且 `499/499` 撤单调用被受理；但一条清理终态 callback 未在有界窗口到达，随后记录 `3` 条 callback gap，因此该轮严格判定为**失败**，不得作为性能样本。随后只读 `QueryUnfinishedOrders` 发现 `25` 条未完成记录；受控恢复对 `25/25` 条发出并获受理的撤单，复核仍有 `2` 条记录未从柜台查询中消失。该状态不改变六轮 O1 A/B 的提交延时结论，却阻塞把这次默认化补充轮标为账户清理验收通过。后续在发起新的账户级 burst 前，必须先以 EMT 权威查询完成这 `2` 条记录的对账或终态确认；不得通过忽略 gap、降低 cleanup 标准或重复盲目撤单来掩盖它。

### O2：callback direct-write A/B——减少无必要交接

候选路径是在每个 scope 自己的 callback 进程中，将 callback envelope 直接写入该 scope 的 mmap Journal，并把复杂投影留给后续 actor：

```text
现行候选：native callback → ingress queue → target worker → callback Journal → C7
试验候选：native callback → route lookup → callback Journal → minimal delivery / projector queue
```

前提与约束：

- callback native 线程只拷贝固定字段、进行无锁化的已发布映射读取、写入固定 frame 和短通知；
- Python 不能诚实地宣称“无锁 ring”。若使用队列，必须说明其实现、生产者/消费者模型及锁语义；
- 不能在 callback 中做 JSON、hash、Redis、风险计算、策略回调或同步日志；
- callback Journal 先 commit，后允许 C7；若 Journal 不能接收，保留 fail-closed 行为；
- 保留当前双 worker 作为默认回退路径，先以显式 feature flag 对比。

**验收方法：** 固定主机、固定四 scope、固定 affinity、三组交错 control/candidate 499-burst；每组均需 499/499 native acceptance、callback、C7 与有界清理闭合。仅在 `C5→C5D`、`C5A→C5B`、`C5→C7` 的 P95/P99 均优于 control，且无完整性回退时，才切换默认值。

### O3：scope-local 负载均衡与容量证明

保持每 scope FIFO 和单 native caller，以稳定的 `execution_id` 路由而非运行时竞争抢占。对于一个 burst，计划器应在 C0 前产生接近平衡的 shard 计数，并报告每 scope 的实际 ingress/native/callback 数量。

当前四 scope 对 499 笔的理论均分为 125/125/125/124。更进一步降低 `C0A→C0B` 的数量级瓶颈只有两种合法方向：

1. 获得并验证额外独立 EMT scope/session，增加真实 native lane 数；
2. 在 C1 前使用明确容量预算拒绝超额信号，而不是无界排队。

第 5 scope 在没有新的 client scope、ownership 证据、零委托 readiness、账户级限流和 callback 归属证明前不得启用。即便五等分，平均队列深度也只从约 125 降至约 100；这不是 1 ms 的承诺。

### O4：OMS 快照、恢复和旁路审计

这一步提升可靠性与可运营性，不进入 C0→C4A 或 C5→C7：

- projectors 从各 scope Journal 构建订单、成交、持仓、资金与未完成订单快照；
- query API 只读取已发布快照，不能触发主动 EMT 查询；
- 启动和断线后的对账 worker 读取 EMT 权威查询并生成 reconciliation fact；
- archive、数据库、指标、告警订阅 committed fact；它们积压只改变运营告警与 admission capacity，不回压已完成的 SDK callback；
- 合并 manifest、segment checksum 与 replay watermark 用于恢复证明，但不在每笔发单中同步刷盘。

## 5. 明确不采用项

| 不采用项 | 原因 |
| --- | --- |
| Redis 重新进入热路径 | 当前 mmap profile 的关键提交段与 callback durable 段已经是微秒级；Redis 只会增加网络/服务端排队与故障域 |
| 牺牲 Journal-first 或改为 fire-and-forget | 无法证明进程故障时是否已调用 EMT，破坏副作用闭包 |
| 每笔 `fsync` / 同步 `msync` | 将 SSD 物理确认加入报单路径，显著牺牲延时；不属于当前“进程崩溃恢复”的目标 |
| vn.py 全局事件队列 | 单线程队列与逐 handler 广播会将 OMS/UI/订阅者耦合到执行延时 |
| WonderTrader 目标仓位算法塞入 adapter | 会把算法、锁、遍历和日志带入 C0→C4A |
| 同一 EMT session 多 native caller | 当前没有可靠性和性能证据支持；违反已采用的 session ownership 约束 |
| 共享服务器的全局 IRQ/内核调度修改 | 可能影响其他项目；只能在隔离维护窗口和专属主机上实验 |

## 6. 验收标准与发布门禁

### 6.1 通用正确性门禁

每一候选版本必须满足：

1. 499 条输入均有可核对结果：native accepted、明确 pre-C1 overload 或权威 EMT throttle；不得无界等待。
2. 每条已 native accepted 订单均有准确 scope identity、client reference、external ID（若 SDK 返回）、C5/C6/C7 记录和完整 callback 配对。
3. 无重复 native `InsertOrder`、route mismatch、callback gap、未知 callback 误归属、Journal 损坏或清理后未闭合订单。
4. 进程中断恢复演练覆盖 `PRE_NATIVE`、`CALL_STARTED`、SDK 返回前、SDK 返回后、callback 前和 callback 后；所有恢复动作必须由 Journal 和 EMT 权威查询证明。
5. 报告只保留测试序号、时间点、分片编号和无载荷统计，不写账户、标的、价格、订单号、密码或密钥。

### 6.2 性能门禁

| 模型 | 目标 | 比较规则 |
| --- | --- | --- |
| 热态单笔 | 识别 O1 对常量成本的作用 | 报告 C0→C4A、C5→C7 全分位数；不得只报平均值 |
| 499 burst | 验证全接纳条件下四分片的排队与 callback | 三轮交错 A/B，报告每段和 batch wall-clock；候选不能以完整性换取分位数 |
| 499 cleanup | 将撤单与报单测量隔离 | 最后一笔发单后等待规定冷却时间，再限速撤单；撤单结果单列 |
| 恢复/故障 | 验证 Journal/Projector 语义 | 不以性能样本代替恢复证明 |

报告至少包含：`count/P50/P95/P99/P100`、每 scope 队列深度、C0→最后 C4A wall-clock、C0→最后 C7 wall-clock、首 C5→最后 C7 wall-clock、每个子阶段完整时间对，以及 host runtime 信号。不同订单的总体分位数不能相减作为某条订单的精确时长。

### 6.3 回退规则

所有优化均由 feature flag 控制，并保留当前已验证路径：

- O1 出现 handle identity/epoch 违规，立即拒绝该命令并退回动态安全路径；
- O2 任一 callback 缺口、C7 先于 Journal commit、P95/P99 回退或 A/B 结果不稳定，保持现行双 worker；
- O3 新 scope 未通过零副作用预检，不创建交易会话、不提交订单；
- O4 的 projector/archive 故障不得改变已经提交的事实，只触发状态告警与 admission gate。

## 7. 实施优先级

1. **O0 callback 精确拆段与无测量污染证据。**
2. **O1 PreparedEmtSubmit。** 低风险、改善单笔常量路径，但不夸大为 burst 排队优化。
3. **O2 callback direct-write 交错 A/B。** 当前唯一可能明显降低 callback 项目内尾延时的纯 Python 架构实验。
4. **O4 OMS projector / replay / snapshot。** 在不改变热路径的前提下补齐可靠性与查询能力。
5. **O3 增加 scope 的容量证明。** 仅在 EMT 提供额外可签署 scope 后开展；这是降低 499 burst native-lane 排队的主要数量级路径。

## 8. 参考实现定位

- Kungfu EMT adapter 的同步报单、双向 ID 映射和回报归属：[trader_emt.cpp](../../../../参考/xlquant_broker/kungfu-systems/kungfu/extensions/emt/src/cpp/trader_emt.cpp)。
- Kungfu Journal 的 frame 填充、close/notify 提交顺序：[writer.cpp](../../../../参考/xlquant_broker/kungfu-systems/kungfu/framework/core/src/libkungfu/yijinjing/journal/writer.cpp)。
- Kungfu TD 对 `OrderInput` 的事件消费与 writer hook 状态更新：[tradervendor.cpp](../../../../参考/xlquant_broker/kungfu-systems/kungfu/framework/core/src/libkungfu/wingchun/broker/tradervendor.cpp)。
- vn.py 的只读 OMS 内存投影：[engine.py](../../../../参考/xlquant_broker/VeighNa/vnpy/vnpy/trader/engine.py)。
- WonderTrader 的策略→executer 路由、local executor 与交易 adapter 分层：[WtExecMgr.cpp](../../../../参考/xlquant_broker/WonderTrader/wondertrader/src/WtCore/WtExecMgr.cpp)、[WtLocalExecuter.cpp](../../../../参考/xlquant_broker/WonderTrader/wondertrader/src/WtCore/WtLocalExecuter.cpp)、[TraderAdapter.cpp](../../../../参考/xlquant_broker/WonderTrader/wondertrader/src/WtCore/TraderAdapter.cpp)。

现行 mmap profile 的实现状态、实测数据与已拒绝的历史优化，仍以 [Mmap-EMT延迟优化实现状态](../03-EMT接口性能/05-Mmap-EMT延迟优化实现状态.md) 为准。本文件定义后续 OMS/EMS 综合优化的设计与验收要求，不以设计文字代替实测验收。
