# BenchmarkStandardV1 复验报告（2026-09-11）

> 结论：当前 Linux `MmapJournalProfileV1` 在三个独立 EMT scope、独立 Python
> process、每 session 一个 serial native caller 的固定拓扑下，通过了 `3 × 499`
> `EXEC-BURST` 与一次 `OMS-W7` 复验。三轮 burst 均为 `499/499` native accepted、
> `499/499` C7、完整 C5→C7 时间对、零 callback gap、`499/499` 撤单受理及 terminal
> cleanup 闭合；W7 同时达到 OMS fact-ingest `2,000 TPS` 和 immutable snapshot `5,000
> QPS`，零 HTTP failure、零 dropped iteration。该结论仅适用于模拟盘、`DISARMED`
> profile 和本次固定环境；不构成 `READY`、自动恢复、生产 SLA 或跨主机 HA 证明。

本报告按 [统一性能压测标准](../05-统一性能压测标准.md) 的 `EXEC-BURST`、`OMS-W7`
口径编写。百分位均是同一轮、同一批逐笔时间对的 nearest-rank 值；三轮摘要仅取“每轮同一
percentile 的中位数”，绝不是合并 1,497 条样本后的 percentile。

## 1. 环境、拓扑与安全门禁

| 项目 | 本轮固定值 |
| --- | --- |
| 代码 | 执行热路径 revision `596c1ae`；最终复验服务器 HEAD 为 `1c114551`（只新增 one-shot recovery proof 隔离退出与本报告，不改执行热路径） |
| 运行时 | Linux x86_64，项目独立 CPython `3.10.21` `.venv` |
| 执行 profile | `TC-EMT-499` / `MmapJournalProfileV1` / EMT 模拟盘 / `DISARMED` |
| 执行拓扑 | 3 个独立 Python process、3 个 EMT API/session、3 条独立 mmap Journal 与 callback worker；499 条稳定分配为 `167 / 166 / 166` |
| 并发约束 | 每个 EMT session 只有一个 serial native caller；不在同一 API 对象上并发调用 SDK |
| CPU 计划 | EMT scope 的 native / ingress / callback-ingress / callback-Journal 分别固定在 0--11 的互斥 CPU；GC 只在 C0--C7 窗口关闭 |
| SDK 与计划 | Linux SDK hash allowlist、已审批模拟盘 fixture、无业务载荷 order-plan 均在 C0 前验证；报告只保留脱敏摘要 |
| 预检与清理 | 本轮前的只读 `QueryUnfinishedOrders` 完整结束、未完成数为 0、撤单尝试为 0；每一轮后的新单均限速清理到 terminal/inflight `0` |

### 1.1 本轮工具异常的处理边界

一次早期 W7 尝试未进入有效验收：EMT 命令使用了脚本入口而非包模块入口，且 k6 被错误地
限制在一个 CPU，导致 EMT 没有写出报告、OMS 出现 EOF/dropped。该轮被标记为**无效诊断样本**，
未写入任何通过结论，也未复用其数字。

诊断还发现：只读 `QueryUnfinishedOrders` 已持久化完整零未完成报告后，vendor Python binding
可能在析构关闭阶段不返回。为此新增仅供该一发性只读工具使用的
`--mutation-recovery-isolate-after-report`：它只在报告与脱敏 trace 均落盘后退出，不改变查询、
委托或撤单策略。最终复验已实际使用此选项，查询进程正常退出。它不是恢复成功的替代证明，
也不影响执行热路径。

## 2. `EXEC-BURST`：三轮 499 笔基线

每轮均完成完整性与清理门禁：`native_accepted=499`、`C7=499`、`C5→C7 pairs=499`、
`callback_gaps=0`、`cleanup_cancel_accepted=499`、terminal inflight remaining `=0`。表中
`C0→C4A` 与 `C5→C7` 是两段项目内部累计延时；`C0→C7` 包含 C4→C5 EMT 外部异步等待，
只作完整观察路径报告。

| 轮次 | C0→C4A P50/P95/P99/P100（ms） | C5→C7 P50/P95/P99/P100（ms） | C0→C7 P50/P95/P99/P100（ms） | C0→last C4A / C0→last C7 / first C5→last C7（ms） |
| --- | --- | --- | --- | --- |
| 1 | 6.901249 / 10.467239 / 11.009991 / 11.198279 | 7.761770 / 12.674921 / 13.457658 / 13.816428 | 46.546419 / 76.438204 / 76.866227 / 76.990180 | 11.198279 / 76.990180 / 65.000782 |
| 2 | 5.889756 / 9.175463 / 9.597096 / 9.788078 | 8.496910 / 13.349302 / 13.525823 / 13.661744 | 76.496002 / 85.569905 / 86.141710 / 86.148660 | 9.788078 / 86.148660 / 70.776608 |
| 3 | 6.147508 / 9.361550 / 9.938372 / 10.111649 | 7.171742 / 8.648967 / 10.309481 / 10.724024 | 47.272306 / 55.182801 / 55.618195 / 55.693857 | 10.111649 / 55.693857 / 44.810027 |
| **三轮 per-run percentile median** | **6.147508 / 9.361550 / 9.938372 / 10.111649** | **7.761770 / 12.674921 / 13.457658 / 13.661744** | **47.272306 / 76.438204 / 76.866227 / 76.990180** | **10.111649 / 76.990180 / 65.000782** |

所有轮次满足当前标准的 mmap commit（P99 ≤ 1 ms、P100 ≤ 3 ms）、callback worker queue
（P95 ≤ 30 ms、P99 ≤ 50 ms）与 C5→C7（P50/P95/P99/P100 ≤ 15/40/60/80 ms）门槛。
`C0→C4A` 不跨历史 scope/CPU/SDK profile 做绝对回归判定。

## 3. `TC-EMT-HOT-100`：热态低并发基线

所有 100 个样本全局一次只有一个在途订单；每笔 C7 与 terminal cleanup 完成后才释放下一笔。
结果为 `100/100` native accepted、`100/100` C7、100 个完整 C5→C7 时间对、零 callback gap、
100/100 cleanup accepted、terminal inflight `0`。

| 项目内部路径 | P50（ms） | P95（ms） | P99（ms） | P100（ms） |
| --- | ---: | ---: | ---: | ---: |
| C0→C4A | 0.417703 | 0.675877 | 2.313234 | 2.482161 |
| C5→C7 | 0.453847 | 1.214804 | 3.327078 | 3.698002 |
| C0→C7（含 EMT 外部等待） | 11.100315 | 13.464866 | 15.275547 | 16.183922 |

这组数据不与 499 突发排队分布混算；它只描述已预热、单笔无队列竞争的调用体验。

## 4. `OMS-W7`：2,000 TPS + 5,000 QPS 与真实 499-burst 并行

W7 使用五个独立 OMS sidecar shard（各一个 benchmark scope），与 EMT 的 0--11 CPU
计划隔离；k6 使用独立的 30--47 CPU 集。OMS 不读取或写入 EMT C2/C6 writer，只消费其自身
合成、脱敏 benchmark fact。负载持续 60 秒。

### 4.1 OMS 调用方完整 HTTP 往返

| 指标 | 结果 |
| --- | ---: |
| 实际 HTTP 请求数 / 平均速率 | 420,004 / 6,999.053 req/s |
| fact-ingest | 2,000 TPS；P50/P95/P99/max = 0.293085 / 3.671493 / 64.031272 / 234.885494 ms |
| immutable snapshot query | 5,000 QPS；P50/P95/P99/max = 0.164454 / 3.803382 / 32.309675 / 196.747889 ms |
| HTTP failure / dropped iteration | 0 / 0 |

### 4.2 OMS 服务端 HDR（不能跨 shard 合并 percentile）

| shard | fact admitted | snapshot query | fact admit→projected P95/P99（ms） | immutable lookup P95/P99（µs） |
| --- | ---: | ---: | ---: | ---: |
| 1 | 23,949 | 60,130 | 0.528895 / 9.977855 | 1.903 / 2.895 |
| 2 | 23,950 | 59,899 | 0.419327 / 4.702207 | 2.003 / 2.795 |
| 3 | 23,962 | 59,938 | 0.506623 / 6.176767 | 1.903 / 2.735 |
| 4 | 24,052 | 60,023 | 0.387839 / 4.988927 | 1.973 / 2.975 |
| 5 | 24,088 | 60,013 | 0.356351 / 2.267135 | 1.923 / 2.665 |

服务端 HDR 与 HTTP 完整往返分别报告：前者定位 actor/projector 或 immutable lookup，后者还包含
loopback TCP、HTTP、JSON 与调用方调度。两者均不计入 EMT C0--C7。

### 4.3 同窗口 EMT 499-burst

W7 EMT 结果为 `complete=true`，`native_accepted=499`、`C7=499`、完整 C5→C7 pair `=499`、
`callback_gaps=0`、`cleanup_cancel_accepted=499`、cleanup terminal 完成。以下为同一 499 条
原始时间对的分位数：

| 路径 | P50（ms） | P95（ms） | P99（ms） | P100（ms） |
| --- | ---: | ---: | ---: | ---: |
| C0→C0A admission | 0.875367 | 1.423389 | 1.488555 | 1.508224 |
| C0A→C0B native queue | 5.071573 | 8.057235 | 8.421675 | 8.581602 |
| C1→C2（含 local route / mmap） | 0.015623 | 0.018578 | 0.033129 | 0.140228 |
| C1M0→C1M1（纯 PRE_NATIVE mmap commit） | 0.004527 | 0.006620 | 0.010435 | 0.079688 |
| C3→C4（SDK 同步调用，仅报告） | 0.004116 | 0.007191 | 0.013040 | 0.032498 |
| C4→C4A accepted Journal | 0.007411 | 0.010085 | 0.029524 | 0.048832 |
| **C0→C4A 项目内提交** | **5.990354** | **9.442949** | **9.757445** | **9.852455** |
| C4→C5 EMT 外部异步等待 | 39.565781 | 50.273884 | 50.535802 | 50.596401 |
| C5→C5D native ingress→Python drain | 5.959197 | 12.201164 | 12.884356 | 12.982121 |
| C5A→C5B callback worker queue | 2.654182 | 5.035229 | 5.192892 | 5.271759 |
| C5M0→C5M1 callback mmap commit | 0.005228 | 0.010165 | 0.020440 | 0.046819 |
| C5M1→C6 callback durable completion | 0.095071 | 0.167288 | 0.216511 | 0.269449 |
| **C5→C7 项目内 callback** | **10.596117** | **16.121021** | **16.312495** | **16.396870** |
| **C0→C7 直接全链路** | **57.382234** | **70.747033** | **71.233754** | **71.340522** |

同批墙钟：`C0→last C4A = 9.852455 ms`、`C0→last C7 = 71.340522 ms`、
`first C5→last C7 = 60.773077 ms`。`C4→C5` 是 EMT 异步回调等待，因此不加到两段项目内部
延时中；`C0→C7` 已直接配对测得，不能由各分段 percentile 相加。

## 5. 验收判定与当前未闭合项

| 项目 | 判定 | 依据 |
| --- | --- | --- |
| 静态质量 | 已通过 | 本地完整质量门禁为 `726 passed, 22 skipped, 1 warning, 84 subtests passed`；部署后 Linux 为 `734 passed, 14 skipped, 1 warning, 84 subtests passed`。两端均通过 Ruff、format、Pyright 与 `git diff --check`。 |
| `TC-EMT-499` | 通过 | 三轮均满足全量接受、C7、时间对、zero gap 与清理门禁。 |
| `TC-EMT-HOT-100` | 通过 | 100 个逐笔闭环、零 gap、清理归零。 |
| `OMS-W7` | 通过 | k6 exit `0`、zero failure/drop、五 shard HDR 完整；并行 EMT 499-burst 全部闭合。 |
| R1-P2 权威恢复证书 | **阻塞** | EMT 模拟盘 `onQueryPosition` 返回 `last=false` 行却未给文档要求的 `last=true` 终态。TradeCore 保持 fail-closed，不以静默或行数推断完成。 |
| R1-P4 A/B R2 真故障闭环 | **阻塞** | 依赖 R1-P2 的完整证书和经验证的 `RESTART` callback replay；不能用当前局部成功或本次性能结果替代。 |
| P5 callback direct-write A/B | 未实施、默认关闭 | 只完成资格合同；必须在 R1 完整证据后以 control/candidate 交错三组验收，不得为了性能绕过 sole-writer。 |
| F5（真实 PaperBroker/QMT） | 明确延期 | EMT-only 范围不包含真实外部 adapter。 |

原始 payload-free 逐笔时间对、W7 k6 summary、每 shard HDR 和清理证明保存在受控服务器运行归档；
Git 只保存本摘要和不含业务/账户/密钥的证据索引。下一项需要外部条件的是 EMT 对
`onQueryPosition` 的版本化终态语义或修复；在此之前，R1 必须持续标记为 `RECOVERY_BLOCKED`。
