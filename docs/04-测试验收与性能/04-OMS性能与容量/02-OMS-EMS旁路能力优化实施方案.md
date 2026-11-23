# TradeCore OMS / EMS 旁路能力优化实施方案

> 状态：P0--P4 已完成代码与本地契约验收；P5 已完成默认关闭的 candidate writer 代码组合、资格合同及 2026-09-14 真实 EMT callback 的三组交错 Control/Direct A/B。六轮均完整闭环，但 Direct 在 `C5→C7` 的 P95/P99 不优于 Control，故**不通过默认路径晋级判据，默认仍为 Control，candidate 保持关闭**；P6 的 OMS load/fault 平台已完成 Linux W0--W6 验收。2026-09-10 的 F4 已在五个隔离 OMS shard 与三个 EMT session scope 下完成 W7：`2,000 TPS + 5,000 QPS` 同时运行真实 EMT 499-burst，零 HTTP failure、零 dropped iteration、`499/499` native/C7、零 callback gap、清理与验收后 query-only 均为零未完成订单。此前 W7 `BLOCKED` 段落是历史记录，不得覆盖当前 F4 manifest。
>
> 范围：在不延长 EMT-only `C0→C4A` 提交路径或 `C5→C7` callback 路径的前提下，补齐订单事实投影、恢复、EMS 编排、只读查询快照，并试验 callback direct-write。
>
> 前提：当前 Python mmap profile 的 `PreparedEmtSubmit`、四独立 session/process 分片、Journal-first、稳定 scope ownership、callback 完整性门禁仍是不可改变的执行基线。本方案不启用 PaperBroker，不扩大 EMT scope，不改变柜台限流，也不将同一 EMT session 改成多 caller。

## 0. 历史实施状态（2026-09-08；不覆盖当前结论）

| 阶段 | 代码状态 | 当前验收状态 |
| --- | --- | --- |
| P0 | 已实现：`OmsSidecarFactV1`、mmap structural reader、scope/epoch/route/position/哈希校验 | 单元负例与重放 fixture 已通过；真实 499 尚待账户清理门禁解除 |
| P1 | 已实现：`OrderStateProjectorV1`、per-scope watermark、checksum-linked manifest chain、immutable generation | restart/replay 等价、position gap 与 manifest 链断裂负例已通过；真实 499 无回归待测 |
| P2 | 已实现：版本化状态机、callback 先于 SDK 返回、重复成交去重、双向 ID 冲突进入 `RECONCILE_REQUIRED` | 逻辑矩阵已通过；真实模拟盘 order/cancel/query 对账待账户门禁解除 |
| P3 | 已实现：bounded `ImmutableOmsSnapshotStoreV1` 与只读 `OmsSnapshotQueryServiceV1` | generation 原子发布与索引一致性已通过；并发查询伴随 499 待测 |
| P4 | 已实现：snapshot freshness gate、bounded EMS pending-plan、算法 child plan 与 attested scope routing | EMS 关闭不进入 adapter；fresh/stale/满载契约已通过；真实 EMS 计划验收待测 |
| P5 | 已实现：默认 control；同 scope sole-writer / sealed-route / bounded inbox 成立时，process-isolated child 内由 candidate writer 直接写入 callback Journal，绕过目标 SPSC 交接；feature 默认关闭 | 真实三组交错 A/B 已完成，六轮均 `499/499` native/C7、零 gap、清理闭合；但 candidate 的 `C5→C7` P95/P99 回退，未达到默认晋级门槛，保持 control |
| P6 | 已实现：独立进程 OMS fact-ingest、immutable snapshot query、watermark/health、HDR 指标、k6 W0--W5、JMeter HTTP 复刻与 W6 fault drill | W0--W6 已通过；F4 使用五个隔离 shard 的 W7 也已通过，历史四 shard / 阻断记录保留为可追溯样本 |

实现位于 `xuanling_tradecore/oms/sidecar_v1.py`、`xuanling_tradecore/ems/control_plane_v1.py` 与 `xuanling_tradecore/ems/callback_direct_write_v1.py`。独立压测服务及合同位于 `tools/oms_load_platform/`。它们只在旁路中读取**已提交**的 mmap 事实；没有修改 C2/C6 writer，也没有将 project/query/EMS 锁、I/O 或对象构建置入 C0--C7。

### 0.1 历史部署与验收记录

- 已推送并以服务器 checkout 的 `git pull --ff-only` 部署提交 `b34352e`；本地完整回归为 `608 passed / 21 skipped`，服务器定向旁路回归为 `10/10` 通过。源码始终由 Git 拉取；k6 仅作为该隔离验收环境的负载生成器。
- W0--W5 在四个独立 OMS 进程/CPU shard 上连续运行 `30 s` 并通过，原始 k6 summary、clean-baseline reset 与每 shard `HDRHistogramV1` 已归档于 `artifacts/oms-load/w0-w5-final-b34352e/`。W1 实际完成 `60,002` 次 fact ingest（`1,999.95 TPS`），HTTP P95/P99 为 `9.441 / 50.336 ms`；W2 实际完成 `150,004` 次 immutable snapshot query（`4,999.58 QPS`），P95/P99 为 `0.571 / 3.044 ms`。
- W3 同时完成 `210,001` 次请求（约 `7,000 TPS+QPS`）、`0` dropped iterations、`0` HTTP failures：fact-ingest HTTP P95/P99 为 `9.810 / 58.965 ms`，snapshot query HTTP P95/P99 为 `5.758 / 23.512 ms`。W3 前的受保护 reset 会关闭 admission、排空已受理事实并恢复固定 seed baseline；它避免将 W1 的合成唯一 ID 累积误当作 W3 的并发性能退化。
- W3 每 shard 的内部 HDR 不伪合并：fact admit→projected P50 为 `0.0588--0.0618 ms`、P95 为 `1.101--1.780 ms`、P99 为 `7.217--11.592 ms`；immutable order lookup P50/P95/P99 为 `1.302--1.342 / 1.923--1.983 / 2.995--3.065 µs`。snapshot materialize/publish 仍是旁路工作（P95 `26.006--34.570 ms`），不进入 EMT C0--C7。
- W4 watermark/health 为 `7,500` 次、零失败（HTTP P95/P99 `0.302 / 0.768 ms`）；W5 证明 pause 后 fact 与 fresh snapshot 都 fail-closed，随后恢复。W6 已实测通过 replay、projector pause、snapshot stale、resume 与 resume 后 replay。
- 【历史记录】当时的 EMT 模拟账户 query-only 清理证明返回 `2` 条未完成记录，W7 启动器因而在未发送订单、撤单或启动 k6 W3 前保护性返回 `BLOCKED`。该记录保留以证明 fail-closed 门禁，不代表当前状态。
- 【当前结论】2026-09-11 W7 已重新通过：五个隔离 OMS shard 与三个 EMT session scope 并行，OMS `2,000 TPS + 5,000 QPS` 下零 HTTP failure/零 dropped，EMT `499/499` native/C7、零 callback gap，清理与验收后 query-only 均为零未完成订单。详见[BenchmarkStandardV1 复验报告](./05-BenchmarkStandardV1复验报告-2026-09-11.md)。W7 不替代 P5；P5 的真实 A/B 已在 2026-09-14 完成，结论为保持 control，详见[真实 Callback Direct-Write A/B 验收记录](./09-V11-真实Callback-DirectWrite-A-B-验收-2026-09-14.md)。

## 1. 目标与不可协商边界

本方案借鉴 Kungfu 的 Journal / TD / Ledger 分层、WonderTrader 的策略 EMS / executor 分层及 vn.py 的 OMS 查询聚合方式，但不迁入任何框架运行时。目标是提供以下五项能力：

1. 从 Journal 可重放地构建 `OrderStateProjector`；
2. 用版本化订单/成交状态机恢复双向 ID 映射；
3. 将目标仓位、拆单和执行算法隔离为 EMS；
4. 提供 immutable OMS 查询快照；
5. 以 feature flag 验证 callback direct-write 是否能安全减少一次队列交接。

所有能力必须服从下列非干扰合同：

```text
C0 → C4A  发单关键路径
C5 → C7   callback durable 路径
```

- 任何 projector、EMS 算法、快照构建、查询、archive、数据库、Redis、告警、报表和 UI 都只能消费**已提交事实**，不得成为上述两条路径的同步前置或同步后置等待。
- `C2` PreDispatch mmap frame commit 前不得创建 EMT 原生请求或调用 SDK；`C6` callback mmap commit 前不得交付 C7。
- 不得在 C0--C7 添加每笔 `fsync` / `msync`、文件扫描、JSON、网络 RPC、数据库调用、同步日志、全局队列、全局互斥锁或账户查询。
- 所有队列、replay buffer、snapshot history、EMS pending plan 都必须有明确容量、版本、监控水位和 fail-closed 结果；不能以无界积压交换低延时。
- 副作用事实的真相来源始终是各 scope 的 Journal 与 EMT 权威查询；OMS/EMS read model 不是发单权威，也不能凭自身数据补发或撤销订单。

## 2. 目标架构

```text
策略 / 上游目标
  └─ EMS control plane
       ├─ 目标仓位、拆单、算法、执行计划
       └─ 已冻结 ExecutionInstruction
                 │
                 ▼
        TradeCore execution hot path
        C0 → C2 mmap → C3/C4 EMT
                 │
        EMT callback → C5 → C6 mmap → C7
                 │
                 ├──────────────────────────┐
                 ▼                          ▼
       scope-local fact reader        callback direct-write A/B
                 │                          （仅候选）
                 ▼
       OrderStateProjector ──► immutable OMS snapshots
                 │                        │
                 ├─ merge manifest        └─ query API / UI / monitor
                 ├─ replay watermark
                 └─ reconciliation worker ──► EMT read-only authority
```

`ExecutionInstruction` 一旦进入 C0 即不可被 EMS 改写。撤单、替换或新的 child order 必须作为新的、已审计 instruction 重新进入既有 admission 与 Journal-first 路径。

## 3. 共用数据与隔离合同

### 3.1 事实记录

每个 scope 仅追加本 scope 的事实；writer 只产生固定 schema 的 payload-normalized record。最小记录集如下：

| 事实 | 产生边界 | 必需关联键 | 作用 |
| --- | --- | --- | --- |
| `PRE_NATIVE_COMMITTED` | C2 | execution ID、canonical client ref、scope、route version | 证明 SDK 调用前归属已落盘 |
| `NATIVE_CALL_STARTED` | C3 前 | execution ID、journal position | 恢复时区分未调用与不确定调用 |
| `NATIVE_ACCEPTED` | C4/C4A | execution ID、external order ID（若返回） | 补全双向关联 |
| `ORDER_CALLBACK` | C5/C6 | client ref / external order ID、状态版本 | EMT 权威订单状态 |
| `TRADE_CALLBACK` | C5/C6 | external trade key、external order ID | 成交去重与累计 |
| `CANCEL_ACCEPTED` / `CANCEL_CALLBACK` | C4/C6 | cancel instruction、目标 external ID | 撤单副作用闭包 |
| `RECONCILIATION_RESULT` | 异步 | authority query epoch、差异结论 | 恢复后的权威对账 |

事实只记录已脱敏的标识摘要、版本、Journal position 和必须的标准化状态；不得将密码、原始 SDK 对象、指针或无界错误文本写入事实或性能样本。

### 3.2 关联与 ownership

主关联为：

```text
execution_id ↔ canonical_client_order_ref ↔ external_order_id
```

- `execution_id → client ref` 在 PreDispatch 前已登记并通过 sealed route map 校验；
- `client ref → execution_id` 必须在 callback 先于 SDK 返回时可查；
- `external order ID → execution_id` 仅在 SDK 返回或 callback 中出现后补充；
- 每个关联均带 `scope_id`、adapter instance、activation epoch、route version 和 Journal position；跨 scope、跨 epoch 或反向冲突立即进入 `RECONCILE_REQUIRED`，不得猜测归属；
- projector 使用 immutable map generation 发布索引；writer 不持有 projector 锁，query 不持有 native-lane 或 callback-writer 锁。

### 3.3 异步背压规则

各旁路消费者需要独立的 bounded inbox。消费者积压不会阻塞已开始的 C0--C7：

| 消费者 | 满载行为 | 对热路径影响 |
| --- | --- | --- |
| Projector reader | 记录 watermark lag、暂停该 reader 的新工作 | 既有事实继续追加；查询明确返回 snapshot 落后位置 |
| Snapshot publisher | 只保留最新待发布 generation | 不阻塞 writer；丢弃的是过期 snapshot 构建任务而非事实 |
| EMS planner | 拒绝或合并尚未进入 C0 的新目标更新 | 已进入 C0 的 instruction 不受影响 |
| Archive / analytics | 按 Journal position 续读 | 只能告警或收紧未来 admission，不能回压 callback commit |

只有 Journal writer、callback gap、scope ownership 或系统关键内存水位故障，才允许 fail-closed 阻止**新的** C0 admission。已经 C3 后的命令必须留给 Journal 恢复和 EMT 权威对账，不得为了回收资源而重复调用 SDK。

## 4. 能力一：OrderStateProjector、merge manifest 与 replay watermark

### 4.1 设计

每个 scope 由一个 `FactReader` 按 append position 顺序读取 native/callback Journal。`OrderStateProjector` 只消费 reader 已验证的 committed frame：校验 schema version、长度、CRC、commit word、scope ownership 与 position 连续性。

```text
scope Journal segments
   → per-scope FactReader
   → ordered fact stream
   → OrderStateProjector
   → immutable state generation + replay watermark
```

`merge manifest` 不是全局热路径 Journal。它在 segment rotate 或异步 checkpoint 时产生，记录：

- `scope_id`、journal generation、segment range、schema hash、checksum；
- 已验证起止 position、projector applied watermark、snapshot generation；
- 创建时间、writer instance/activation epoch 与 manifest version；
- 前一 manifest 摘要，形成可审计链。

其写入发生在 snapshot/checkpoint 旁路。发单与 callback writer 仅追加自身事实，不等待 manifest 或 projector 更新。

### 4.2 启动与恢复

1. 加载最后一个 checksum、schema 和 ownership 均有效的 manifest；
2. 从该 manifest 的 `applied_watermark + 1` 顺序重放各 scope segment；
3. 发现 segment 缺口、CRC 失败、route epoch 不匹配、无效状态迁移或 ID 冲突时，停止自动推进并标为 `RECONCILE_REQUIRED`；
4. replay 完成后创建新 immutable snapshot generation；
5. 由独立 read-only reconciliation worker 查询 EMT 权威未完成订单、订单状态、成交和资金/持仓快照；
6. 对账结果以 `RECONCILIATION_RESULT` 事实写回 Journal，projector 再消费它，而不是直接篡改 read model。

恢复 worker 不得发出 submit，也不得自动取消外部订单；自动取消只可作为独立、审批过的控制命令。

### 4.3 验收

- 正常 append、segment rotate、进程中断后的 replay 结果与连续运行结果在状态、ID 索引、watermark 上完全相同；
- 注入截断尾帧、坏 CRC、重复 frame、跨 scope frame、manifest 缺口和过期 epoch 时，必须 fail-closed；
- replay 期间 C0→C4A、C5→C7 的 499-burst 对照组 P95/P99 不得回退；
- projector 停止、落后或重启时，writer 的 C2/C6 路径不等待 projector；
- 只读对账的业务数据不出现在性能报告或日志中。

## 5. 能力二：订单/成交状态机与双向 ID 重建

### 5.1 状态模型

状态机由 projector 执行，采用单调、版本化转换。建议内部状态：

```text
INTENT_ACCEPTED
  → PRE_NATIVE_COMMITTED
  → NATIVE_CALL_STARTED
  → NATIVE_ACCEPTED
  → WORKING / PARTIALLY_FILLED / FILLED
  → CANCEL_PENDING / CANCELLED
  → REJECTED / EXPIRED / LOST / RECONCILE_REQUIRED
```

- `NATIVE_CALL_STARTED` 不是“委托已被柜台接受”；
- `NATIVE_ACCEPTED` 只表示 SDK 主动调用取得返回；
- terminal 状态只能来自可验证的 EMT callback 或权威 reconciliation fact；
- 同一 callback 可重复；以 venue 状态版本、sequence 或稳定 payload hash 去重；
- 成交使用柜台 trade key 去重，累计数量与金额只单调增加；
- 非法逆转、同一 ref 绑定多个 external ID、同一 external ID 绑定多个 execution 或跨 scope callback 均进入 `RECONCILE_REQUIRED`。

### 5.2 ID 索引重建

projector 在每个 immutable generation 中发布三个只读索引：

```text
by_execution_id
by_client_order_ref
by_external_order_id
```

索引条目只保存 `OrderStateRef`：scope、事实 position、状态版本和轻量摘要；完整历史按需由 reader/replay 按 position 读取。这样查询不会复制整段 Journal，更不会回到 native lane 查询 SDK。

### 5.3 验收

- callback 在 SDK 返回前、返回后、重复、乱序、断线重连、进程重启后均能得到一致状态；
- 订单与成交的重复回放不改变累计数量、成交去重表或公开 snapshot generation；
- 任何无法安全重建的关联只产生 reconciliation gate，不产生 SDK 重发；
- 100 次随机 frame cut/replay 与至少一次真实模拟盘 order/cancel/query 对账均通过；
- 状态机和 ID map 的 CPU/内存只记入 projector 旁路 telemetry，不计入 C0--C7。

## 6. 能力三：WonderTrader 式 EMS control plane

### 6.1 分层

EMS 在 instruction 进入 C0 前完成策略和执行决策，不能作为 adapter 内部逻辑：

```text
目标仓位 / 风险预算 / 执行指令
  → PositionNetter
  → ChildOrderPlanner
  → ExecutionAlgorithm (TWAP/VWAP/POV/passive)
  → ExecutorRouter
  → frozen ExecutionInstruction
  → TradeCore admission / Journal-first / EMT
```

- `PositionNetter` 读取已发布 OMS snapshot，不读 native session，不占用 callback writer；
- `ChildOrderPlanner` 输出有限、版本化 child order plan；每个 child plan 都有 TTL、最大数量、最大金额、risk policy hash 与 source snapshot generation；
- `ExecutionAlgorithm` 可定时或事件驱动，但仅排队将来 instruction；它不得等待 SDK、callback 或 projector catch-up；
- `ExecutorRouter` 只选择已经 ownership-attested 的 execution scope，最终 route fence 仍由 C0 重新验证；
- EMS 取消、替代、暂停或风险 stop 产生新的控制 instruction，禁止修改已经 journaled 的订单。

### 6.2 非干扰机制

1. EMS 与 execution process 使用独立 actor/thread/process；
2. 传入 C0 的对象已冻结，热路径不调用目标仓位、拆单、风险遍历或策略回调；
3. 每个策略/账户有有界 pending-plan 容量；满载时合并尚未发布的目标更新或拒绝新计划，不允许冲掉已发送委托；
4. snapshot watermark 落后超过阈值时，EMS 停止产生新单并报告 `EMS_SNAPSHOT_STALE`；不阻塞已提交订单的 callback；
5. 审计、指标与解释日志从 control-plane event stream 读取，不能在 C0--C7 生成。

### 6.3 验收

- EMS 启动、停止、算法超时、snapshot 落后、计划队列满、重复目标更新均不增加现有 hot-path P95/P99；
- 所有生成的 child instruction 仍通过原有 account fence、scope ownership、价格保护、频率和 Journal-first 门禁；
- 在 EMS 关闭时，直接 `ExecutionInstruction` 路径功能与性能完全不变；
- 模拟盘中验证目标变更、暂停、撤单与恢复不会重复下单；
- EMS 本身单独压测，报告 plan latency、合并数、拒绝数和 snapshot watermark，不与 EMT C0--C7 混合。

## 7. 能力四：vn.py 风格的统一 OMS 查询快照

### 7.1 查询模型

查询 API 面向 immutable `OmsSnapshot`，而不是面向 adapter：

```text
OmsSnapshot {
  snapshot_generation,
  per_scope_applied_watermark,
  as_of_monotonic_ns,
  orders_by_execution,
  orders_by_client_ref,
  orders_by_external_id,
  trades_by_key,
  positions,
  balances,
  health
}
```

- publisher 在 projector 完成一个一致 generation 后，以单次原子引用替换发布 snapshot；
- query reader 获得一个 generation 后只读，不加 writer 锁；
- 返回值必须标出 `snapshot_generation`、每 scope watermark、`as_of` 与是否 stale；
- 分页、筛选和排序对 snapshot 的局部索引进行，不能触发同步 EMT query；
- 需要柜台权威数据时，单独创建 async reconciliation job；调用方得到 job/reference，而不是阻塞 C0/C5。

### 7.2 一致性与内存

- 保留固定数量的 recent snapshot generations；被 reader 引用的 generation 以引用计数或 epoch 安全回收；
- full order history 留在 Journal / archive，snapshot 只保留当前状态与有限索引；
- snapshot 超过容量时只淘汰旧 generation，不淘汰未投影的事实；
- projector 未赶上时查询返回最近已验证 generation 与明确 lag，而不是读取部分可变 map。

### 7.3 验收

- 并发查询、分页和订阅不持有 native / callback writer 锁；
- 持续查询压测下，499-burst 的 C0→C4A 与 C5→C7 P95/P99 无回退；
- generation 切换期间 reader 不见半成品、空索引或同一订单双状态；
- restart/replay 后快照 generation 与 watermark 与事实流一致；
- 查询 API 遇到 stale、scope gap 或 reconciliation required 时显式拒绝“权威实时”声明。

## 8. 能力五：callback direct-write A/B

### 8.1 目的与安全路径

当前 callback 经过 ingress、sealed route、目标 lane，再由 journal worker 写入 callback mmap。candidate 只允许移除**已证实多余**的目标 lane 交接：

```text
control
SDK callback → bounded ingress → Python drain → target SPSC lane → callback Journal → C7

candidate
SDK callback → bounded ingress → Python drain / sole Journal writer → callback Journal → C7
                                                               └→ projector inbox
```

candidate 的 direct writer 是已经从 SDK callback 边界脱离的 scope-local Python drain worker，**不是**未证明可重入的 SDK callback thread。它仍须是该 callback mmap Journal 的唯一 writer；若 callback ownership 需要跨 scope 转交、writer 正忙、route 未 sealed 或队列处于 fail-closed，则立即使用 control 路径或停止新单，绝不进行不受控多 writer 写入。

### 8.2 候选热路径限制

direct writer 只允许：

1. 读取已发布的 sealed route；
2. 复制固定、长度受限的标准化 callback 字段；
3. 填充预分配 frame、校验并提交 mmap commit word；
4. 更新极小的 immutable/atomic callback state；
5. 将已提交 position 放入 bounded projector inbox，随后发出 C7。

禁止：JSON、对象图遍历、dataclass/UUID 构建、Redis、数据库、磁盘同步刷盘、策略回调、同步日志、账户查询、跨 scope 路由竞争或以 Python 自称无锁 MPSC。

### 8.3 A/B 与回退

- 默认保持 control；candidate 使用 profile feature flag，仅允许 `DISARMED` 模拟盘验收；
- 固定同一主机、四 scope、CPU plan、SDK、Python、callback pipeline、fixture 和 499 负载，执行三组交错 control/candidate；
- 每组须 `499/499` accepted、`499/499` C7、完整 callback 子段配对、0 gap、0 callback writer corruption、清理 terminal 闭合；
- candidate 必须在 `C5→C5D`、`C5A→C5B`、`C5→C7` 的 P95/P99 同时不劣于 control，且不能以 C7 早于 C6、丢弃 projector position 或弱化恢复证据换取延时；
- 任一 writer ownership 违规、gap、checksum 失败、回放差异、P95/P99 回退或结果不稳定，立即关闭 feature flag 并保留 control。

### 8.4 2026-09-12 阻断记录与 2026-09-14 实际 A/B 结论

2026-09-12 的历史窗口中，实际 Control 499-burst 在首个 C5 原生订单 callback 前失败闭环：`499/499` 仅代表同步 native accepted，`C5/C7=0/499`，因此 Direct 没有启动，不能产生比较结论。供应包同构 `TraderApi` 探针同样没有订单类 callback；两次事后权威查询均确认未完成委托为零。完整无载荷记录、恢复条件和通用 armed gate 见 [V1.1 真实 callback direct-write A/B 阻断记录](./07-V11-真实Callback-DirectWrite-A-B-阻断记录-2026-09-12.md)。这是一条历史阻断证据。

2026-09-14 的有效交易窗口先以一笔 TradeCore 完整运输探针确认 `C5→C7`，再以同一受控模拟盘 fixture、同一 revision、三个 process-isolated EMT session scope、相同 CPU role plan 和相同 499 共同 C0，执行 `Control-1 → Direct-1 → Control-2 → Direct-2 → Control-3 → Direct-3`。六轮均为 `499/499` native accepted、`499/499` C7、零 callback gap、完整 callback 子段时间对与清理 terminal 闭合；事后权威 query-only 为零未完成委托。实际性能比较显示 candidate 虽将 `C5A→C5B` 的中位 P95/P99 降至亚微秒级，但三轮中位 `C5→C7` P95/P99 为 `10.237125/11.949286 ms`，高于 Control 的 `9.329204/10.913194 ms`，不满足 8.3 的同时不劣于判据。因此该试验**完成了真实 A/B 验收和回退决策，结论是保留默认 Control**，而不是把 candidate 写成性能优化。完整的脱敏逐轮指标与判定见[真实 Callback Direct-Write A/B 验收记录](./09-V11-真实Callback-DirectWrite-A-B-验收-2026-09-14.md)。

## 9. 分阶段实施顺序

| 阶段 | 交付 | 是否触及 C0--C7 | 进入下一阶段门槛 |
| --- | --- | --- | --- |
| P0 | 事实 schema、ID contract、sidecar telemetry、无载荷 replay fixtures | 否 | schema/CRC/route/epoch 负例覆盖完成 |
| P1 | `OrderStateProjector`、watermark、manifest、replay reader | 否 | restart/replay/reconcile matrix 通过，499 无回归 |
| P2 | 单调订单/成交状态机、双向索引 generation | 否 | 乱序/重复/断线/冲突矩阵通过 |
| P3 | immutable `OmsSnapshot` 和 query API | 否 | 查询压测与 499 同时运行无热路径回归 |
| P4 | EMS planner / executor router / child plan | C0 前 | EMS 关闭时零差异；启用后所有 instruction 仍经既有 gate |
| P5 | callback direct-write feature-flag A/B | 仅 C5--C7 候选 | 三组交错 A/B、replay 等价和回退演练全部通过 |

P1--P4 可并行开发，但上线顺序必须是 P0 → P1 → P2 → P3 → P4 → P5。P5 最后实施，因为它是唯一改变 callback 交接形态的候选。

## 10. 统一性能与可靠性验收

### 10.1 性能口径

- 热态单笔：至少 100 条，报告 `C0→C4A`、`C5→C7` 与完整子段 `count/P50/P95/P99/P100`；
- 突发：499 条共同 C0，四 scope/process，报告完整 499 条时间对、三项 batch wall-clock、每 scope queue depth 与 runtime telemetry；
- 旁路压力：对 projector、query、EMS 分别制造高水位，同时执行同口径 burst；
- 对比：同一环境、同一 fixture、同一 CPU plan、同一 SDK/ABI；不得把 EMT `C4→C5` 外部等待归因为项目回归或优化；
- 结论：每轮单独计算 nearest-rank 分位数，再比较轮次中位数；不得混合池化后伪造改善。

### 10.2 可靠性口径

1. 任何 `PRE_NATIVE`、`NATIVE_CALL_STARTED`、SDK 返回、callback、manifest rotate、projector apply、snapshot publish 时的进程中断均可恢复或进入 `RECONCILE_REQUIRED`；
2. 任何 gap、route collision、外部 ID 冲突、manifest 缺失或 checksum 失败均不能生成新的 SDK 调用；
3. 旁路进程崩溃不得阻塞已有 C0--C7，也不得让 query 显示未经验证的状态；
4. EMS 故障只停止未来计划生成，不能改写或重复已 journaled instruction；
5. callback direct-write 与 control 对同一注入事实流必须产生一致的 Journal frame 序列、状态机结果、watermark 与 snapshot 内容。

## 11. 明确不做的事项

| 不做项 | 原因 |
| --- | --- |
| 将 projector / OMS 查询放进 adapter.submit 或 callback writer | 会把旁路 CPU、锁和 I/O 引入 C0--C7 |
| 用数据库或 Redis 作为 Journal 提交确认 | 会引入网络往返、服务端队列和额外故障域 |
| 用 OMS snapshot 直接触发撤单/补单 | snapshot 不是柜台权威，可能重复副作用 |
| 将 EMS 算法和目标仓位遍历放进 native lane | 使策略复杂度放大 native queue 尾延时 |
| SDK callback thread 直接运行完整 OMS / Python 策略代码 | 破坏 SDK 边界、GIL 调度与 callback 完整性 |
| 无界队列或无版本 mutable global dict | 无法定义背压、恢复边界和一致性 |
| 为降低尾延时取消 Journal-first / C6-before-C7 | 破坏进程故障恢复与副作用闭包 |

## 12. 参考实现边界

- Kungfu：借鉴 Journal append/close、scope-local writer、订单输入与回报关联、旁路 Ledger/replay；不迁入 C++ runtime、Master/Ledger/UI 或其 TD 进程模型。
- WonderTrader：借鉴 `target → planner → executor → adapter` 分层；不迁入目标仓位计算、策略 DSL、日志和历史遍历到 EMT execution adapter。
- vn.py：借鉴统一 OMS 只读视图；不迁入全局单队列事件引擎或在查询中同步访问柜台。

与现行执行基线的关系见 [Python OMS/EMS 综合热路径优化方案](./01-Python-OMS-EMS综合热路径优化方案.md)；当前 mmap 实测与既有优化结论见 [Mmap-EMT 延迟优化实现状态](../03-EMT接口性能/05-Mmap-EMT延迟优化实现状态.md)。本文件只定义后续五项旁路能力的设计和验收，不表示它们已经实现或通过验收。
