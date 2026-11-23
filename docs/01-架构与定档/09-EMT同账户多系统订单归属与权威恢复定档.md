# EMT 同账户多系统订单归属与权威恢复定档

> 状态：**2026-09-11 已在冻结 revision `75fa900` 完成 R1-N1--N4 的受控演练：三 scope 查询终态、精确关联 `RESTART` 见证、A/B 同账户隔离、A 的 R2 强杀且 B 持续、A4/A5 故障矩阵及 A6 人工精确清理均已留存脱敏 evidence。** 结论严格停在 `RECOVERED_DISARMED`，`full_r1_acceptance=false`、`ready_claim=false`；有限精确回放见证不构成任意历史订单的全面 replay 证明。本文件将 EMT-only 的 manual-only 恢复扩展为“同一资金账户存在多个独立交易系统时，TradeCore 仅识别、重建和处置自己订单”的设计基线。它不改变已完成 F0--F6 的验收结论，不授权自动恢复、自动撤单、自动重报或 `READY`。
>
> 适用范围：Linux、项目独立 CPython 3.10、当前 EMT SDK、模拟盘优先。所有实施和故障演练均须保持 `DISARMED`，并使用脱敏证据。

## 1. 问题、目标与非目标

### 1.1 问题

在 R2 故障边界中，`InsertOrder()` 已同步返回，但本地 `NATIVE_ACCEPTED` / Pending 映射尚未完成持久化。重启后，EMT 权威查询可能返回一笔本地 OMS 暂时未知的委托。

若同一 EMT 账户还运行其他系统，不能把“本地未知”直接视为 TradeCore 自己的订单，更不能据此撤单、投影到策略或覆盖本地状态。恢复必须先判断订单归属，再使用 EMT 查询重建属于当前 TradeCore 实例的映射。

### 1.2 目标

1. 用 EMT 回传的订单级关联字段，对每笔查询/回调订单做可验证的系统归属分类。
2. 在 R2 后从 `order_client_id` 反向定位已提交的 PreDispatch route，重建 `execution_id ↔ order_client_id ↔ order_emt_id`。
3. 将其他系统订单保留为可审计的 foreign evidence，但绝不投影、撤单、重报或计入本系统 callback gap。
4. 遇到未知、碰撞、查询不完整或关联不唯一时 fail-closed，结果最多为 `RECOVERED_DISARMED`，绝不自动进入 `READY`。
5. 命名空间分配、恢复查询与策略归属查找均不进入 C0--C7 的同步外部 I/O；热路径只执行已验证的本地编号分配与 Journal 写入。

### 1.3 非目标

- 不把 TradeCore 的 `client_id`/`order_client_id` 机制强加给其他柜台或未来 QMT/PaperBroker profile。
- 不以本方案替代 EMT 官方字段回显、分页完成语义或多 session 账户级限流的实际 capability probe。
- 不在恢复阶段自动撤销外部订单、自动重发不确定订单、自动解除资金冻结或自动恢复交易。
- 不在本功能包中扩大真实交易权限；模拟盘完成的证据不等同生产 SLA。

## 2. 字段语义与术语冻结

必须避免把 EMT 会话标识与每笔订单标识混用。

| 名称 | EMT/TradeCore 含义 | 本方案职责 | 不能承担的职责 |
| --- | --- | --- | --- |
| `emt_client_id` | 创建 EMT Trader API 时使用的会话/client scope；当前实现按 `uint8` 校验。 | 优先作为系统/独立 session 的一级隔离键。 | 不能在未证明其随订单查询回显前，单独作为订单归属证据。 |
| `order_client_id` | TradeCore 发单时使用的非零 `uint32`，并映射到 canonical client order reference；EMT 查询历史行可回传 `0`（调用方未设置该字段）。 | 非零值才可作为订单级反向路由主键、命名空间租约成员和 R2 恢复匹配键。 | `0`、缺失值或缺少本地 Journal/route evidence 时，不能凭数值猜测策略，必须归为 `UNKNOWN_NAMESPACE`。 |
| `order_emt_id` | EMT 同步受理后返回的外部订单 ID。 | 与 `order_client_id` 成对绑定；用于撤单和二次校验。 | R2 前可能尚未写入本地，因此不能作为唯一恢复起点。 |
| `strategy_id` | TradeCore 策略的稳定身份。 | 由已提交的本地 Route Ledger 反向取得。 | 不强制编码进 32 位 `order_client_id`。 |
| `execution_id` | TradeCore 子单/执行指令的稳定身份。 | 连接策略、PreDispatch、OMS 和外部订单。 | 不代替 EMT 原生 ID。 |

当前静态契约声明 `order_client_id` 应出现在订单/成交/撤单错误 callback 以及分页订单查询路径；这只是实现前提，必须由本功能包的实际 EMT 模拟盘 probe 逐项证明。若任一路径不回显、字段不稳定或分页语义不能闭合，则本功能包失败并保持 manual-only recovery。

## 3. 总体架构

```text
签名 Scope Ownership              签名 OrderClientId Namespace Manifest
account + emt_client_id     +      account/day/client/range/owner/expiry
            │                                      │
            └──────────────启动期验证───────────────┘
                                           │
策略 → ExecutionInstruction → 预分配 order_client_id → PreDispatch Route Ledger + mmap
                                                           │
                                                           ▼
                                                     EMT InsertOrder
                                                           │
                 ┌──────────────R2 崩溃───────────────────┘
                 ▼
启动恢复：关闭新副作用 → Journal/Route 重建 → EMT 权威双扫描
                                                   │
                 ┌─────────────────────────────────┼────────────────────────────────┐
                 ▼                                 ▼                                ▼
            OWN_MATCHED                       OWN_R2_ORPHAN                 FOREIGN_RESERVED
            重建本地读模型                    补 external-ID 映射             仅审计，零副作用
                 │                                 │                                │
                 └──────────────────────┬──────────┴────────────────────────────────┘
                                        ▼
                          UNKNOWN / COLLISION / incomplete query
                                        │
                           RECOVERY_BLOCKED + DISARMED
```

## 4. 命名空间与路由定档

### 4.1 一级隔离：已签名的 `emt_client_id` scope

优先为每个独立系统分配不同的 EMT `emt_client_id`。当前的 signed scope ownership 继续绑定：

```text
venue=EMT + account + adapter_instance_id + emt_client_id
```

一个 live scope 只允许一个已声明的 TradeCore owner。`emt_client_id` 的不同不代表可绕过账户级风控、EMT 限流或账户清算；是否允许同账户多 session 并行仍须由模拟盘 capability probe 证明。

### 4.2 二级隔离：`OrderClientIdNamespaceManifestV1`

即使每个系统具有不同 `emt_client_id`，也必须为 `order_client_id` 保留显式租约，防止配置错误、未来合并 scope 或柜台回显不足导致歧义。

每个 manifest 条目至少包含：

```text
schema_version
venue_id = EMT
account_scope_hash
trading_day
emt_client_id
namespace_id
owner_system_id
adapter_instance_id
range_start / range_end              # 闭区间，1..2^32-1
manifest_version / valid_from / valid_until
release_manifest_hash
signer_id / certificate_reference / revocation_reference / signature
```

同一 `(account_scope_hash, trading_day, emt_client_id)` 下，所有有效范围必须严格不重叠。采用**范围租约**而不采用固定 bit packing：策略数量和日内峰值无需压缩进 32 位；系统可按已测容量申请连续区间；热路径只做本地范围边界检查与递增分配。

范围不得在日内重用。交易日切换前，必须完成上一交易日的权威查询、回放闭合和 manifest 封存；否则新日 scope 不得启动新副作用。

### 4.3 `RouteLedgerV2`：策略归属的权威本地线索

在 PreDispatch mmap frame 完整提交时，同时写入可从 mmap 重建的 route fact：

```text
(account_scope_hash, trading_day, emt_client_id, order_client_id)
  → owner_system_id
  → adapter_instance_id
  → strategy_id
  → execution_id
  → journal_epoch / frame_sequence
  → request_digest
  → namespace_manifest_hash
```

`strategy_id` 不要求编码进 `order_client_id`。R2 的恢复假设是 PreDispatch 已提交而 Pending 映射未写入；因此该 route fact 已存在，足以安全重建策略归属。若连 route fact 也不可验证，则订单最多可分类为“本系统 namespace 的未解析订单”，不得猜测策略或继续交易。

### 4.4 运行期 ownership fence

现有 account-level ownership fence 不可被悄然放宽。实施时新增 `ExecutionNamespaceFenceV1`，其逻辑 scope 至少为：

```text
account_scope_hash + trading_day + emt_client_id + namespace_id + manifest_hash
```

它只处理 TradeCore 控制根内的并发 owner；不同系统或不同主机的排他性依赖已签名 manifest 的非重叠校验和发布流程，不能把本地 `flock` 误表述为跨系统分布式锁。manifest 不存在、签名失效、范围冲突或 owner 不匹配时，启动必须失败并保持 `DISARMED`。

## 5. 权威查询与恢复算法

### 5.1 权威数据集

恢复模式先关闭新提交与新撤单，然后采集、持久化摘要并校验完成语义：

1. 当日分页订单全量查询；
2. 未完成订单查询；
3. 当日成交查询；
4. 资产、持仓及信用状态查询（作为成交/冻结差异辅助证据）；
5. recovery 期间持续写入的原生 callback ingress。

若 EMT 不提供带 `as-of` 语义的统一快照，使用受控**稳定双扫描**：完成 A 扫描、未完成订单/成交扫描和 B 扫描。当前 namespace 的订单投影以及任何 `UNKNOWN_NAMESPACE` 投影必须在两次扫描中逐 `(order_client_id, order_emt_id, status, filled, leaves)` 一致；分页终止语义和 callback gap 也必须闭合。已签名 `FOREIGN_RESERVED` 范围的增删或状态演进单独以两次摘要、数量和 drift 计数留存：它绝不投影、撤单或进入本系统状态机，因此不阻断仅重建当前 namespace 的 `RECOVERED_DISARMED` 结论。任何当前 namespace 漂移、未知 tag、分页不完整、负确认缺失或 callback gap 都是 `RECOVERY_BLOCKED`。

受控验收在 `InsertOrder()` 同步返回后，先用有限次数的只读全量订单查询确认本系统的 `order_client_id` 已唯一可见，才开始 A/B 双扫描。该可见性等待只存在于 recovery/acceptance 旁路；到达上限仍不可见即 `RECOVERY_BLOCKED`，不得以同步返回或 callback 代替权威查询证据。

### 5.2 分类规则

每个 EMT 订单记录以 `(trading_day, emt_client_id, order_client_id)` 作为首选分类键；若 EMT 查询不回传 `emt_client_id`，只可使用已签名范围与本地回放作辅助，且必须降低为更严格的未知处理。

| 分类 | 条件 | 恢复动作 | 是否允许副作用 |
| --- | --- | --- | --- |
| `OWN_MATCHED` | 命中当前 namespace，且 Route Ledger 与外部 ID/订单字段唯一一致。 | 重建映射并投影 OMS。 | 否；恢复阶段仅重建。 |
| `OWN_R2_ORPHAN` | 命中当前 namespace，存在可信 PreDispatch/`CALL_STARTED`，但缺 `NATIVE_ACCEPTED` 或 Pending 映射。 | 以 EMT 记录补建 `order_emt_id` 映射，记录 R2 evidence。 | 否。 |
| `FOREIGN_RESERVED` | 命中其他有效、已签名 namespace 或另一个已证明的 client scope。 | 仅写脱敏摘要与数量；不进入本地 OMS。 | 否，绝不撤单。 |
| `OWN_UNRESOLVED` | 命中本系统范围，但没有唯一可信 route fact。 | 输出差异并封锁恢复。 | 否。 |
| `UNKNOWN_NAMESPACE` | 不属于任何有效 namespace。 | 输出差异并封锁恢复。 | 否。 |
| `NAMESPACE_COLLISION` | 同一个归属键映射到多个 route 或 manifest range 重叠。 | 封锁恢复并保留完整审计。 | 否。 |

“foreign”不等于“安全忽略一切”：必须进入恢复 evidence 的计数与摘要，以证明本系统没有误占用它；但它不应构成本系统的 callback gap，也不得触发自动撤单。

### 5.3 R2 的精确处理

```text
可信 PreDispatch: execution_id + strategy_id + order_client_id
        ↓
InsertOrder() 已返回，进程在 Pending/NATIVE_ACCEPTED 持久化前终止
        ↓
重启后 EMT 查询命中本系统 namespace 的 order_client_id
        ↓
RouteLedgerV2 命中唯一 PreDispatch fact
        ↓
从 EMT 读取 order_emt_id / 状态 / 已成交量，补建 native 映射
        ↓
写入 RECOVERY_REHYDRATED（非原始历史帧）和不可变 evidence
        ↓
OMS 读模型重放；状态保持 RECOVERED_DISARMED
```

`RECOVERY_REHYDRATED` 必须是独立恢复事实，包含原始事实引用和 EMT 查询摘要，绝不回写或伪造旧 PreDispatch/NATIVE_ACCEPTED frame。

## 6. 实施包与验收顺序

本定档作为后续独立功能包 **R1：EMT 多系统归属与权威恢复**。R1 的所有代码、迁移、脚本和定向测试完成前，不运行真实 EMT 集中验收；完成后一次性按 A1--A5 执行。

### R1-N1：Capability 与 manifest 编译器

- 实现 `OrderClientIdNamespaceManifestV1`、canonical encoding、签名验证、过期/撤销验证和范围重叠检查。
- 以实际 EMT 模拟盘证明 `order_client_id` 在订单、成交、撤单错误、分页订单查询、未完成订单查询中的回显和稳定性。
- 实现静态 scope/range 发布前检验；拒绝未签名、重复 owner、过期、范围耗尽和冲突配置。

### R1-N2：分配器、Journal 与 route ledger

- 将当前单调 allocator 改为只在已签名租约范围内分配。
- 在 PreDispatch 记录 namespace/owner/strategy/execution/manifest hash 的可恢复 route fact。
- 新增 namespace fence；保留现有 account-level fence，直到并发语义和部署拓扑通过验收。
- 修改 callback/query normalizer，使 foreign order、trade、cancel error 都能按 namespace 分类，而非仅处理已知 foreign cancel callback。

### R1-N3：EMT 权威恢复编排

- 实现完整查询 family、分页终止校验、稳定双扫描、callback replay 合并和脱敏 recovery evidence。
- 实现上述六类分类、R2 rehydrate、negative confirmation 与所有 fail-closed gate。
- 恢复 API 只能输出 `RECOVERED_DISARMED`、`RECONCILING` 或 `RECOVERY_BLOCKED`；不存在自动 `READY` 路径。

### R1-N4：统一验收

所有 N1--N3 完成后，在固定 Linux 模拟盘环境一次性完成：

| 验收编号 | 场景 | 必须结论 |
| --- | --- | --- |
| R1-A1 | 静态质量、全量回归、manifest encode/signature/range contract | 默认严格质量门禁零错误。 |
| R1-A2 | 两个系统同账户、不同 client scope 或 namespace 的正常下单与查询 | 全部订单准确分类；foreign 零投影、零撤单。 |
| R1-A3 | 系统 A R2 强杀；系统 B 的订单持续存在 | A 仅重建自己的外部 ID/策略 route；B 订单不被触碰。 |
| R1-A4 | 重叠 range、伪造签名、过期 manifest、未知 tag、重复 route | 启动或恢复 fail-closed；无新副作用。 |
| R1-A5 | callback/分页中断、双扫描不稳定、查询超时、负确认缺失 | `RECOVERY_BLOCKED`；无自动重报/撤单/READY。 |
| R1-A6 | R2 后恢复、限速清理与终态核对 | 仅经明确授权清理 `OWN_MATCHED`/已人工确认的本系统订单；foreign 保持不变。 |

### 6.1 历史首轮 EMT 模拟盘证据（2026-09-10）

已实现：签名 namespace directory/范围冲突校验、本地 namespace fence、范围约束分配器、PreDispatch mmap `RouteLedgerV2`、五标量权威订单扫描、双扫描、foreign/unknown/collision 分类和严格 `RECOVERED_DISARMED` 上限。新增的 `SignedNamespaceReleaseV1` 在 SDK/session/allocator 之前完成严格 release JSON、独立签名/撤销、scope、runtime release hash、trust-anchor hash、有效期、首次 bootstrap approval 及 epoch/high-watermark anti-rollback 验证；公共 execution composition 仅接受其 `ActivatedNamespaceReleaseV1` 输出。查询扫描以 `(order_client_id, order_emt_id)` 去重；同一 client ID 指向不同外部 ID 会成为 `NAMESPACE_COLLISION`，不会被误当成 SDK 回调损坏。

R2 成功候选现在生成带 `recovery_run_id`、A/B scan digest、manifest hash、route payload hash 与原始 `journal_epoch/frame_sequence` 的 `RECOVERY_REHYDRATED` 事实。事实必须先由 mmap `begin → facts → commit` 批量写入；读端只在 batch id、数量和 canonical digest 全部匹配后才幂等重建 native ID 映射。缺 sink、写入异常、未提交 batch、route reference 不匹配或任一 blocking reason 均保持 `RECOVERY_BLOCKED`，不会改变内存 route ledger。它仍不是现场 R2 证据。

代码候选 `b174a060` 已在 Linux EMT 模拟盘完成一笔受控 `Guard → route → scheduler → native submit → authoritative query → cancel → terminal cleanup`。脱敏 evidence 见 [R1 Linux EMT controlled authority evidence](../../artifacts/acceptance/r1/2026-09-10-r1-linux-emt-controlled-authority.evidence.json)：SDK hash 与 ownership fence 通过；提交前/清理后未完成委托均为零；受控订单在有限只读可见性确认后，在 A/B 全量扫描中各唯一命中一次；两次扫描摘要一致、callback gap 为零、namespace route fact 存在，恢复结论为 `RECOVERED_DISARMED`。测试结束时 session 为 `STOPPED_DISARMED`，没有 `READY` 或自动恢复声明。

该样本是早期历史证据，单独并不等同 R1-N4 完成，也不证明完整 N3 查询证书；其后续补齐与当前统一演练结论见 6.2 和 6.3。无论历史或当前样本，均保持 `DISARMED + manual reconciliation`，不授予自动行为或 `READY`。

验收 evidence 至少保存：release/SDK/profile/manifest hash、查询开始与结束时刻、分页完成摘要、双扫描 digest、分类计数、每个本系统订单的脱敏关联 hash、foreign 摘要计数、callback gap、最终 gate、清理结果和人工结论。

## 7. 性能与安全边界

- manifest 验签、范围重叠检查、双扫描、查询、证据写入与策略目录读取都在启动/恢复/旁路执行，不得放进 C0--C7。
- C0 前加载只读 manifest；C1--C4 热路径仅执行已验证的 range check、递增编号、PreDispatch frame 与现有 native call。
- callback 热路径可使用不可变 namespace 目录进行 O(1) 分类；未知/foreign 只入有界审计队列，不等待 OMS、数据库或网络。
- 如果无法在不影响 C0--C7 的前提下证明新 route persistence 的性能，则保持当前路径和 manual-only recovery，不能以恢复功能名义降低热路径保证。

## 8. 当前实现核验与迁移结论

| 项目 | 当前事实 | R1 处理 |
| --- | --- | --- |
| `order_client_id` allocator | 已是 nonzero `uint32`，按账户/交易日/adapter/client scope 恢复后单调分配。 | 增加签名 range lease，禁止每个独立系统在同一 scope 从 1 开始。 |
| Journal-first | PreDispatch 先于原生调用；R2 已被识别为需要对账的故障边界。 | 扩展为可恢复的 owner/strategy/namespace route fact。 |
| foreign callback | 当前仅对已预登记的 foreign cancel callback 有窄路径处理。 | 覆盖 order/trade/cancel/query 的统一归属分类。 |
| recovery evidence | EMT read-only port 当前刻意返回不完整恢复证据。 | 增加真实 query family、双扫描与 R2 rehydrate evidence。 |
| ownership fence | 当前为本地 TradeCore 控制根内的 account-level fence。 | 增加 namespace fence；不将本地锁误称为跨系统全局协调。 |

### 6.2 2026-09-11 查询终态、精确回放与受控闭环更新

本节覆盖并取代 6.1 中关于“查询终态/`RESTART` replay 尚未实证”的旧状态；6.1 的早期样本仍作为历史失败诊断保留，不能被删除或倒改为通过。

1. **查询完成语义已按 EMT native callback 的 `is_last` 实测闭合。** 三个逻辑 scope 分别完成完整 query-family 矩阵；分页订单扫描以 `onQueryOrderByPage(..., is_last)` 的最后页作为唯一完成证据，其他 query family 以各自 `last=true` 结束。缺 `last=true`、分页 cursor 不一致、未知 request、session 不匹配或静态 row 归一化失败均仍 fail-closed。最新独立 query-only 探针未发单、未撤单、未写恢复事实，得到完整分页订单扫描 `14,087` 行、零 callback gap、零 row-failure 摘要；该计数只是当日脱敏容量证据，不是订单业务断言。
2. **`order_client_id=0` 的查询行不再截断扫描。** 归一化层保留该行的外部订单标识和完整分页进度，但将其分类为 `UNKNOWN_NAMESPACE`；它不能进入 own route、rehydrate、撤单或策略投影。这既防止一个未标记的历史委托使整次扫描伪失败，也防止被错误归为本系统订单。
3. **三 scope `RESTART` 已取得精确关联、签名见证的回放证据。** 普通、信用、期权各自使用受控的既有身份对，在独立 session 中观察到 `exact_witness_callback_count=2` 且 `replay_correlated=true`。probe 不执行 query、submit、cancel 或 payload 持久化，结果保持 `DISARMED`。公共流中的其他/泛 callback 不计入该结论，也不能替代身份关联见证。
4. **最新单系统完整 F2 控制循环通过。** release/SDK/scope gate、受控委托、权威可见性、稳定双扫描、经 guard/route/scheduler 的精确限速清理、terminal callback、当前 F2 精确 `order_client_id` 已不在 `QueryUnfinishedOrders` 中、callback gap 为零及 `RECOVERED_DISARMED` 均已获得脱敏 evidence。账户可能与其他系统共用，验收绝不把“账户全局无未完成委托”当作前提或清理成功条件。它证明当前单系统受控路径，而非 A/B 多系统恢复，也不授予 `READY`。

证据仅存于受限 Linux 控制根的脱敏 evidence 目录；版本库不保存账户、订单、地址、口令、私钥或原始 callback payload。EMT Python 帮助中心仅作为外部帮助入口；本实现的完成判定以随 SDK 发布的 native callback 签名和本轮实际回调时序为准。

### 6.3 2026-09-11 R1-N4 统一演练结果

冻结 revision `75fa900` 已完成同账户双系统的 A2 正常组合、A3 的 R2 物理强杀且 B 保持、A4/A5 的 fail-closed 故障矩阵，以及 A6 的人工批准精确 own cleanup。两套系统均验证了权威双扫描、namespace route fence、callback gap-free 与 cleanup 后未完成归零；最终独立 query-only 核验亦以 native 终态完成、未完成数为零、取消尝试为零。Hot100、三轮 499 burst 和 OMS W7 同一冻结代码复验详见 [R1 统一验收与性能复验报告](../04-测试验收与性能/04-OMS性能与容量/06-R1统一验收与性能复验报告-2026-09-11.md)。

因此，R1 当前具备可验证的 namespace、route ledger、完整查询终态、精确关联 replay、同账户多系统隔离及 manual-only R2 权威扫描/清理演练。有限 exact witness 不证明任意历史对象的全面 callback replay，故 `full_r1_acceptance=false` 且仍按现行规则保持 `DISARMED + manual reconciliation`。

## 9. 与现行路线的关系

- F1 的 mmap Journal 恢复验收继续有效；它证明本地事实恢复与 gate，不等同 EMT 权威查询恢复。
- F2/F4/F6 的 EMT 模拟盘受控副作用、性能和发布候选证据继续有效；它们不证明多系统同账户归属或 R2 authoritative rehydration。
- L2 仍可进行人工发布候选审查，但此处 R1 是申请账户级 `READY` 前的新增权威恢复能力包。
- 任何 R1 实现变更均须重新执行受影响的 F1/F2/F4/F6 验收；不得把旧 revision 的性能或 ABI 证据移植到新 release。
