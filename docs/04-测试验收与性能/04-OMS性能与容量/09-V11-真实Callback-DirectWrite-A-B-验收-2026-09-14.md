# V1.1 真实 Callback Direct-Write A/B 验收记录（2026-09-14）

> 结论：真实 EMT 模拟盘的三组交错 A/B 已完成，六轮均满足完整性、清理和权威事后查询门槛。Direct candidate 大幅移除了 `C5A→C5B` 的本地交接等待，但未同时满足 `C5→C7` P95/P99 不劣于 Control 的晋级条件。因此 feature flag **保持默认关闭，Control 继续为默认路径**。本记录不是 `READY`、生产 SLA 或 EMT 外部回调延时承诺。

## 1. 目的、范围与安全边界

本次仅验证 Python `MmapJournalProfileV1` 下的 callback direct-write candidate 是否可以在不牺牲完整性、清理和项目内尾延时的条件下，替代现有 Control callback 交接。

- revision：`94b92f1`，服务器通过 Git checkout 的 `git pull --ff-only` 获取；
- 柜台：EMT 模拟盘；只使用已签名、受控的模拟盘 order fixture；
- 分片：3 个 process-isolated EMT API/session scope；每个 scope 的 producer、native caller、callback ingress 和 callback writer 使用互不重叠的 CPU role plan；
- 负载：每轮 499 笔共同 `C0` 全接纳 burst，pipeline 上限固定为 8，测量窗口关闭 GC；
- profile：Control 与 `direct-candidate` 除 callback handoff 外保持相同的 revision、fixture、scope、CPU plan、SDK 和负载；
- 安全：真实 burst 仅在受控 fixture 显式 armed 的短窗口执行；完成后已原子解除 armed。所有输出均为无账户、标的、价格、订单号、地址、凭据或私钥的脱敏工件。

`C4→C5` 是 EMT 异步 callback 等待，属于外部柜台/SDK 投递时间；它没有用于判断本地 candidate 是否优化。

## 2. 执行顺序与有效性

在批量运行前，先由当前 TradeCore 路径完成一笔原生 callback 运输探针：`native_accepted=1`、`C7=1`、`C5--C7` 全段覆盖、无在途残留。随后在相同有效交易窗口依次运行：

```text
Control-1 → Direct-1 → Control-2 → Direct-2 → Control-3 → Direct-3
```

每轮均以相同的 hash-bound order-plan 启动。任一轮若出现 callback gap、C7 未满 499、writer corruption、异常回退、清理未终态或权威查询不完整，即应停止序列并维持 Control；本次没有触发这些失败条件。

## 3. 完整性与清理结果

| 检查 | Control-1 | Direct-1 | Control-2 | Direct-2 | Control-3 | Direct-3 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `native_accepted` | 499 | 499 | 499 | 499 | 499 | 499 |
| `callback_completed_C7` | 499 | 499 | 499 | 499 | 499 | 499 |
| callback gap | 0 | 0 | 0 | 0 | 0 | 0 |
| callback 子段时间对 | 499 | 499 | 499 | 499 | 499 | 499 |
| cleanup terminal complete | 是 | 是 | 是 | 是 | 是 | 是 |
| cleanup inflight remaining | 0 | 0 | 0 | 0 | 0 | 0 |
| 本轮验收 | 通过 | 通过 | 通过 | 通过 | 通过 | 通过 |

六轮后的只读权威 `QueryUnfinishedOrders` 查询已完整结束，返回未完成委托 `0`；该查询没有发起撤单或其他账户变更。

## 4. 单笔分位数：三轮中位数

单位均为 ms。每个数值先按单轮 499 条配对样本以 nearest-rank 计算，再取同 profile 的三轮中位数；没有将六轮样本池化。P100 为该轮最大值的中位数。

| 项目内区间 | Profile | P50 | P95 | P99 | P100 |
| --- | --- | ---: | ---: | ---: | ---: |
| `C0→C4A` | Control | 6.520102 | 10.324964 | 10.726341 | 10.876739 |
| `C0→C4A` | Direct | 6.267761 | 9.974979 | 10.604181 | 10.799306 |
| `C5→C5D`（callback ingress 到 Python drain） | Control | 2.547233 | 5.616407 | 9.824564 | 9.959788 |
| `C5→C5D`（callback ingress 到 Python drain） | Direct | 2.311126 | 10.182513 | 11.896086 | 12.108497 |
| `C5A→C5B`（scope-local handoff） | Control | 2.922866 | 5.990153 | 6.234222 | 6.337919 |
| `C5A→C5B`（scope-local direct handoff） | Direct | 0.000381 | 0.000530 | 0.000791 | 0.000931 |
| `C6→C7`（已提交 callback 到下游观察） | Control | 0.001292 | 0.003876 | 0.006610 | 0.011798 |
| `C6→C7`（已提交 callback 到下游观察） | Direct | 0.001833 | 0.003546 | 0.006460 | 0.011437 |
| `C5→C7`（完整项目内 callback） | Control | 7.475479 | 9.329204 | 10.913194 | 10.966696 |
| `C5→C7`（完整项目内 callback） | Direct | 2.364287 | 10.237125 | 11.949286 | 12.161937 |

### 4.1 性能判定

candidate 的局部目标成立：`C5A→C5B` 的 P95/P99 从 `5.990153/6.234222 ms` 降到 `0.000530/0.000791 ms`。这验证了 direct handoff 确实移除了原目标 worker 的排队交接。

但 8.3 的候选晋级要求并不只看局部段：`C5→C5D`、`C5A→C5B` 和完整 `C5→C7` 的 P95/P99 必须同时不劣于 Control。Direct 的完整 `C5→C7` P95/P99 为 `10.237125/11.949286 ms`，相对 Control `9.329204/10.913194 ms` 分别高约 `9.7%/9.5%`；`C5→C5D` 的 P95/P99 也有回退。故不能以更低的 P50 或局部 handoff 结果掩盖尾延时回退。

## 5. 499-burst 整批墙钟时间

单位 ms；每项均以一轮的共同起点和最后一条完成时间直接测量，不能与单笔分位数相加。

| Profile | C0→最后 C4A（中位数） | C0→最后 C7（中位数） | 首 C5→最后 C7（中位数） |
| --- | ---: | ---: | ---: |
| Control | 10.876739 | 69.591575 | 58.501488 |
| Direct | 10.799306 | 66.501963 | 54.704265 |

Direct 的三轮中位整批墙钟略低，但这是异步 callback 窗口中的整批观测，不能替代逐笔 `C5→C7` P95/P99 的晋级标准，也不能用于归因 EMT `C4→C5` 外部等待。

## 6. 逐轮关键值

| 轮次 | `C0→C4A` P95 | `C5→C7` P95 / P99 | `C5→C5D` P95 / P99 | `C5A→C5B` P95 / P99 | C0→最后 C7 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Control-1 | 10.324964 | 9.095171 / 10.913194 | 5.227465 / 10.065531 | 4.905078 / 5.047483 | 65.518246 |
| Direct-1 | 9.969170 | 11.637125 / 12.811022 | 11.527829 / 12.758442 | 0.000491 / 0.000751 | 66.501963 |
| Control-2 | 9.933548 | 9.329204 / 10.712536 | 5.616407 / 9.824564 | 5.990153 / 6.234222 | 73.079539 |
| Direct-2 | 10.373933 | 7.756404 / 8.921256 | 7.699427 / 8.868897 | 0.000541 / 0.000791 | 67.058987 |
| Control-3 | 10.475147 | 12.024980 / 12.231654 | 6.272178 / 6.700366 | 6.902582 / 7.099219 | 69.591575 |
| Direct-3 | 9.974979 | 10.237125 / 11.949286 | 10.182513 / 11.896086 | 0.000530 / 0.000821 | 62.204686 |

## 7. 验收结论与后续约束

1. **真实 callback 运输与 A/B 完整性：通过。** 2026-09-12 的 `C5=0` 阻断是历史外部投递窗口证据；本窗口已由一笔探针和六轮 499-burst 证实 callback 可完整进入 TradeCore。
2. **candidate 默认化：不通过。** 完整 `C5→C7` P95/P99 没有达到不劣于 Control 的门槛，立即回退的正确实现是保持 feature 默认关闭，而不是删除候选代码或改变历史 Control 基线。
3. **安全/清理：通过。** 每轮均清理至 terminal，事后权威 query-only 为零未完成委托；fixture 已解除 armed。
4. **状态边界：仍为 `DISARMED`。** 本次不证明生产 `READY`、自动恢复、掉电耐久性、任意历史 callback replay，或 EMT 柜台端的性能 SLA。

若要再次开启新的 candidate 实验，必须改变 candidate 的 `C5→C5D` 尾延时成因并以新的 revision、同等安全 gate、相同三组交错实际窗口重新验证；不得复用本次“局部交接更快”的结果作为默认化依据。
