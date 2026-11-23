# F1：mmap Journal 恢复与合并规范

> 状态：**F1 已统一验收通过（保持 `DISARMED`）**。最终脱敏证据见 [F1 recovery acceptance evidence](../../artifacts/acceptance/f1/2026-09-09-f1-recovery-acceptance.evidence.json)；此前的 [blocked manifest](../../artifacts/acceptance/f1/2026-09-09-f1-recovery-implementation.blocked.json) 仅保留为历史状态。该结论覆盖 `MmapJournalProfileV1` 的恢复闭环，不授权账户进入 `READY`、自动重报或自动撤单；受控模拟盘验收 harness 的订单与清理不构成恢复 API。

## 1. 目标和不变量

F1 只读取已提交的固定大小 mmap frame，把多个 scope/shard 的可信前缀变成可校验的恢复视图。它解决的是 TD/Python 进程异常退出后的“哪些本地事实已经完成、哪些副作用未知”，不是断电 durability，也不是自动交易恢复。

```text
mmap frame body + CRC → final commit word
          │
          ▼
strict contiguous-prefix scanner
          │        ├─ untrusted tail / CRC / gap → JOURNAL_INTEGRITY
          │        └─ valid facts
          ▼
route / ID reconstruction → reconciliation sets → sidecar / projector replay
          │                                               │
          └─ MergeManifestV2 (sealed hash chain)          └─ rebuildable read model
                                  │
                                  ▼
                    DISARMED + manual reconciliation only
```

不变量如下：

1. 每个 frame 先写 body 与 CRC，最后才写 commit word；scanner 只接受从 sequence 1 开始的连续可信前缀。
2. 首个半写、CRC 错误、非法 kind、错误 commit 或“空 frame 后仍有内容”的位置会记录为 `JOURNAL_INTEGRITY`，不能静默跳过。
3. `ORDER_PRE_NATIVE` 与新增的 `ORDER_CALL_STARTED` 是两个不同事实。后者无 `ORDER_NATIVE_ACCEPTED` 时为 `SUBMIT_UNKNOWN`，必须对账，绝不自动重报。
4. `MmapPreNativeJournal` 仍以实际 writer thread 保证文件单 writer；启动期 `MmapWriterOwnershipRegistryV2` 再保证 scope/shard 只能由一个声明 owner 组合。碰撞即 `WRITER_COLLISION`。
5. 恢复成功只会产生 `DISARMED` 快照。后续必须完成权威 Broker 查询、人工批准与独立 activation 包，F1 本身没有 arming API。

## 2. 文件和版本兼容

现有文件头为 `<8sIIQQ>`：magic、schema version、frame size、capacity、epoch。早期 V1 把最后字段写为 `0`；F1 将其按 epoch `1` 读取，保持旧文件兼容。新建文件写入显式正整数 epoch。固定 `512` 字节 frame 的 profile hash 由实现常量生成，`frame_hash` 则只哈希可信前缀的固定身份字段和 payload digest，不包含 SDK 原始对象、账户密钥或请求正文。

新增 on-disk kind：

| kind | 语义 | 恢复分类 |
| --- | --- | --- |
| `ORDER_PRE_NATIVE` | native 前的身份 fence 已提交 | `PRE_NATIVE` |
| `ORDER_CALL_STARTED` | 已到达调用 EMT SDK 前的最后本地事实 | 缺 receipt 时 `SUBMIT_UNKNOWN` |
| `ORDER_NATIVE_ACCEPTED` | SDK 同步返回 native order ID | `CALLBACK_PENDING` 或已受理 |
| `ORDER_CALLBACK_OBSERVED` | callback mmap commit 已完成 | route 必须可重建 |
| `CANCEL_STARTED` / `CANCEL_NATIVE_ACCEPTED` / `CANCEL_REJECTED` | 撤单副作用事实 | `CANCEL_UNKNOWN` 或 `CANCEL_ACCEPTED` |

## 3. MergeManifestV2

`MergeManifestV2` 的 canonical JSON 使用 ASCII、key sort、无空白，并以 SHA-256 做 `manifest_hash`。新的 manifest 引用前一份的 hash，形成 hash chain。每个已排序 `(scope_id, shard_id)` 条目保存：

- `journal_epoch`、`profile_hash`、`frame_hash`；
- `contiguous_position` 与预分配 `capacity`；
- route directory digest、writer identity digest；
- `generated` 和 `sealed` 状态。

只有所有 shard 均可信、没有 route/控制根/callback/容量/lag/manifest 漂移 gate 时，manifest 才能 `generated=true, sealed=true`。`MergeManifestV2Store` 仅在非热路径使用临时文件、fsync、原子 replace 写入 sealed manifest；它不在 C0--C7 调用。

## 4. 恢复编排与对账

恢复协调器按 `(scope, shard, sequence)` 稳定读取，不凭 wall clock 重新排序。它重建以下最小关联：`execution_id ↔ client_order_ref ↔ command_id/payload_hash ↔ native_order_id`。同一 scope 内 client ref、execution ID、命令/hash 或 native ID 出现冲突即 `ROUTE_CONFLICT`。

| 已知事实组合 | 输出 | 操作限制 |
| --- | --- | --- |
| PRE_NATIVE，无 CALL_STARTED | `PRE_NATIVE` | 不发送；由对账/人工判断是否已副作用。 |
| CALL_STARTED，无 native receipt | `SUBMIT_UNKNOWN` | 强制权威查询；禁止自动重报。 |
| native receipt，无 callback | `CALLBACK_PENDING` | 查询/订阅补齐，禁止假定终态。 |
| callback/cancel 没有 PRE_NATIVE route | `CALLBACK_GAP` | gate，人工检查 route/root。 |
| cancel started，无 cancel receipt/reject | `CANCEL_UNKNOWN` | 对账，不自动重复撤单。 |

可信 records 可由 `MmapSidecarFactReaderV1` 投射为 `PRE_NATIVE_COMMITTED`、`NATIVE_CALL_STARTED`、`NATIVE_ACCEPTED` 等事实，交给 `OrderStateProjectorV1` 幂等重放。该 projector 和后续 PG 都是可重建 read model，不能反向覆盖 mmap 事实。

## 5. 容量、健康与门禁

每个 Journal 从创建时预分配。`MmapJournalCapacityPolicyV2` 在非热路径评价 `stop_new_at_percent` 和最大 projector backlog；达到水位、projector watermark 倒退/超前或 lag 超限均闭合 gate。下列事件都会让 runtime 保持 `DISARMED + manual reconciliation`：

- `JOURNAL_INTEGRITY`、`ROUTE_CONFLICT`、`CALLBACK_GAP`；
- `CONTROL_ROOT` 不完整、`MANIFEST_DRIFT`；
- `CAPACITY`、`PROJECTOR_LAG`、`WRITER_COLLISION`。

`MmapRecoveryGateV2.require_submit()` 是给启动组合注入的 fail-closed seam。F1 中它始终拒绝，不存在“检查通过即放行”的自动路径。

## 6. 验收矩阵

实现和定向单测覆盖多 scope/shard merge、hash chain、V1 epoch 兼容、torn tail、CRC、non-contiguous frame、route conflict、callback gap、control-root 缺失、capacity/lag、writer collision 和 projector 幂等回放。Linux CPython 3.10 + SDK 的三 scope 零委托 session 预检已通过：`signals_target=0`、`outbound_mutation_attempted=0`、3 个 adapter 均启动并隔离退出。

2026-09-09 的统一验收已闭合下列项目：

1. 物理强杀：在 C2 前、C2 后/C4 前、C4 后/C5 前、callback mid-write、cancel mid-write 五个边界对子进程执行 `SIGKILL` 后重开 Journal。五例均得到关闭 admission 的 `DISARMED` 恢复快照；callback/cancel 的两例刻意半写尾帧均关闭 integrity gate。该 drill 不加载 SDK，且零下单/撤单调用。
2. 恢复证据：确定性矩阵验证 sealed manifest/hash chain、容量与单 writer gate、sidecar/projector 幂等重放，以及无自动重报。恢复对象只提供不可变 gate snapshot，并未暴露 submit/resend API。
3. Linux 热态/突发：三 scope、受控模拟盘的 100 次单笔基线为 100/100 native accepted 与 C7、零 callback gap、清理后零在途；499 笔突发为 499/499 native accepted 与 C7、零 callback gap、499/499 清理受理并完成终态。100 次基线 C0→C4A 的 P50/P95 为 0.439293/0.734089 ms，C5→C7 为 0.545284/1.050808 ms；499 突发的两段 P95 分别为 9.882247 ms 与 21.121156 ms，批墙钟分别为 10.744676 ms 与 61.415035 ms。恢复扫描和 projector 不在 C0--C7 热路径，本包以确定性重放正确性而非虚构的同步热路径耗时验收它们。

所有项完成也不得把状态改为 `READY`；本结论只允许进入下一功能包，账户继续 `DISARMED + manual reconciliation`。
