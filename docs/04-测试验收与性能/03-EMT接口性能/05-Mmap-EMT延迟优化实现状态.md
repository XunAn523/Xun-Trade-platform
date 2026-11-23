# Mmap EMT 延迟优化实现状态

更新日期：2026-09-09。本文件记录当前可执行的 EMT 性能 profile；历史 Redis 方案的实现、部署脚本、压测工具和专项测试已从工作树移除，不再构成可运行能力或验收前置。

## 当前唯一执行 profile：`MmapJournalProfileV1`

TradeCore 在副作用前与回调交付前只提交本机预分配 mmap Journal frame：

```text
C0 → C0A admission → C1 → PRE_NATIVE mmap commit (C2) → EMT InsertOrder (C3→C4)
EMT callback (C5) → sealed route lookup / bounded lane → OUTCOME/CANCEL mmap commit (C6) → C7
```

- `C2` / `C6` 表示 body、CRC 写完且 commit word 最后提交的 mmap frame 已可被同一台仍在运行的操作系统上的重启进程读取。
- 热路径不连接 Redis，不执行 Redis Function、数据库写入、`fsync`、同步 `msync`，也不等待异步 archive。
- 路由、提交和回调任一 mmap 提交失败均失败关闭：不跨越 EMT 副作用边界，或不对下游声称已交付。
- mmap 解决的是交易进程崩溃/重启时的本地因果归属，不承诺瞬间断电、内核崩溃、存储故障或跨主机 HA 的最后脏页安全。
- `MmapJournalProfileV1` 仍保持 `DISARMED`，尚不能据此声明 `READY`、自动恢复或生产可用。

实现入口为 `emt_gateway.tradecore_mmap_journal`；性能驱动为 `tools/performance/perfbench/tradecore_mmap_emt_burst.py`，其报告明确使用 `MmapJournalProfileV1`，并将 C1→C2 标记为 pre-native mmap commit、C5→C6 标记为 callback mmap commit。

## 499 条高性能基线：Linux 分片，不要求单 lane 串行

从 2026-09-08 起，TradeCore 的 499 条性能定档基线是 Linux 上的
`process-isolated` 分片组合：三个独立 Python 进程、三个已签署 EMT
session、三个独立 serial native lane，将 499 条稳定分配为
`167 / 166 / 166`。每个 scope 分别固定 native worker、ingress producer、
callback ingress 与 callback Journal worker 的独立 CPU；不共享 API 对象、
Python GIL 或 callback Journal worker。

- 分片 profile 的验收是 499/499 native accepted、499/499 C7、完整 C5→C7
  时间对、0 callback gap、每笔限速撤单受理，且撤单后的 terminal callback
  必须释放全部 new-order inflight credit。
- 报告应同时比较两段项目内部累计延时 `C0→C4A` 与 `C5→C7`，以及整批
  `C0→last C4A`、`C0→last C7` 和 `first C5→last C7` 墙钟完成时间；`C4→C5`
  仍是 EMT 异步等待，不计入项目内部总延时。
- `tight-single-producer` 单 session/lane 是刻意汇聚 499 条队列的架构对照，
  用于衡量串行瓶颈；它不替代上述 Linux 分片基线，也不是当前性能验收前置。

### 本地受控 order-plan 的有效期边界

`historical-controlled-fixture` 仅用于模拟盘、既有受控请求的可比性运行；它不声称
当前行情有效。该类本地计划可以使用 JSON `null` 表示无有效期，以便同一受控参数在
后续回归中复用。`external-read-only-market-preflight` 则必须保留有限有效期：它来自
某一时点的被动价格/涨跌停校验，过期后必须重新只读预检，不能借由无有效期绕过。

两种计划都在 C0 前完整校验，原始计划仅保留在受忽略的本机目录；Git 与性能报告只记录
其 SHA-256。无有效期不扩大账户权限、不改变 `DISARMED`，也不把历史受控参数转换为
实时行情或 `READY` 证据。

## 已完成的清理

1. 删除 Redis Function Journal、Stream reader/router、账户恢复/对账桥、callback gap、route directory 和 stream reset 的旧运行时代码。
2. 删除 Redis UDS/ACL/Function/shard 的主机部署资产、Lua 脚本和 C++ 开放环压测工具。
3. 删除仅验证上述 Redis 路径的单元测试与性能驱动，保留 mmap 实现及其对应的测试。
4. 移除当前 EMT 组合中的可选 Redis projection 与兼容分支；`EmtMmapNativeCallJournal` 是唯一的 native/callback Journal 组合。

## 2026-09-08：三进程 CPU 计划 499 条模拟盘闭环

Git revision `9faa16a` 新增 process-isolated CPU plan：只有同时给出每个 scope 的 native worker、callback ingress、callback Journal 三组 CPU，且全部 CPU 跨角色/跨 scope 不重复时，驱动才会执行；不完整、重复或将 scalar pin 与 plan 混用均 fail-closed。子进程在应用每个线程 affinity 后才可参与共享 C0 的 499 条 burst，聚合报告逐 scope 回显实际计划，避免把“进程隔离”误写成同核竞争。

本轮以 Git clone 建立独立测试工作副本，在 CPython 3.10 `.venv` 和 Linux EMT binding 下运行。三个 scope 分别处理 `167 / 166 / 166` 条；每个 scope 的 native / ingress / journal worker 固定在不同物理 core。结果为 `499/499` native accepted、`499/499` C7、`499/499` 完整 `C5→C7` 时间对、`0` callback gap、`0` producer/dispatch/callback/cleanup error，以及 `499/499` 单次清理撤单获 EMT 接受。

除逐笔分位数外，驱动现在还记录整批墙钟完成时间：共同 `C0` 到最后一笔 `C4`（全部 `InsertOrder` 返回）和共同 `C0` 到最后一笔 `C4A`（全部提交内部路径完成）。这两个值只在所有计划信号拥有同一 C0 与对应完成时间点时写出，不能以部分成功样本伪造 batch complete。本轮既有 499 条原始样本复算为 `C0→last C4 = 23.531070 ms`、`C0→last C4A = 23.542197 ms`。

| 项目内路径（ms，499 个完整样本） | P50 | P95 | P99 | P100 |
| --- | ---: | ---: | ---: | ---: |
| C0→C0A admission | 1.017884 | 1.799574 | 1.947996 | 1.976439 |
| C0A→C0B native scheduler queue | 8.982529 | 16.868956 | 20.564449 | 21.479349 |
| C0B→C0C dequeue 后准备 | 0.002674 | 0.003365 | 0.007131 | 0.047000 |
| C0C→C1 prepared-route proof | 0.002413 | 0.002864 | 0.005158 | 0.020161 |
| **C0A→C1** | **8.988037** | **16.873973** | **20.570759** | **21.485317** |
| PRE_NATIVE mmap commit | 0.005408 | 0.009194 | 0.026710 | 0.118718 |
| **C0→C4A 提交内部总和** | **10.138849** | **18.575361** | **22.604773** | **23.542197** |
| C5→C5D SDK ingress 至 Python drain | 3.861492 | 11.731885 | 12.185424 | 12.275129 |
| C5D→C5E sealed route | 0.019108 | 0.022133 | 0.040240 | 0.067101 |
| C5E→C5A SPSC publish | 0.000922 | 0.001993 | 0.008713 | 0.024076 |
| C5A→C5B callback worker queue | 2.597419 | 4.792175 | 5.050171 | 5.116149 |
| **C5→C7 callback 内部总和** | **7.444215** | **13.454293** | **14.030246** | **14.082684** |
| C4→C5 EMT 异步等待（不计入项目内） | 32.751986 | 39.713919 | 40.501338 | 40.553136 |
| C0→C7 直接测量 | 51.037953 | 64.612898 | 65.479305 | 65.627697 |

相对前一条记录的三进程 499 单轮，`C0A→C1` P95 从 `19.007816` 降至 `16.873973 ms`，`C5→C7` P95 从 `19.205161` 降至 `13.454293 ms`；这只是一次同类模拟盘运行的归因结果，不是跨环境 SLA。窗口中 scope 1/2 的 process migration 为 `0`，scope 3 为 `4`，且主机 IRQ 与 minor page fault 仍有增量。因此本轮证明了代码级 thread affinity、独立 session/process、预构建 route 和 callback SPSC queue 的组合闭环；**未**证明全机 IRQ 隔离、硬实时调度、持续账户级并行能力或 `READY`。

## 2026-09-08：ingress producer CPU 固定与整批 C7 证据

Git revision `cc129f3` 将每个 scope 的 raw-signal ingress producer 纳入 CPU plan。每个 process-isolated scope 现在必须同时提供 native worker、ingress producer、callback ingress、callback Journal 四个 CPU；四组列表缺失、重复、长度与 scope 不一致，或与 scalar pin 混用均 fail-closed。producer 在共同 C0 barrier 前完成 thread affinity，避免用未固定的 producer 线程测量 C0→C0A / C0A→C0B。随后 `c0a547e` 新增最早 `C5_CALLBACK_INGRESS` 到最后一条 `C7_AFTER_DOWNSTREAM` 的 callback batch-drain 墙钟时间。C0→last C4/C4A/C7 只在 499 条全量拥有同一 C0 与完整时间戳时写出；first C5→last C7 则要求每条均有单调 C5→C7 时间对，起点为全批最早 C5，不能用部分回执伪造 batch complete。

本轮仍在隔离 Git worktree、CPython 3.10 `.venv`、Linux EMT binding 与 `MmapJournalProfileV1` 下运行，采用 3 个已签署 scope 的 `167 / 166 / 166` 分配。scope 1/2/3 分别使用 `(180,181,182,183)`、`(184,185,186,187)`、`(188,189,190,191)` 作为 native/producer/callback-ingress/callback-Journal CPU；12 个 CPU 为同 socket 的不同物理 core，报告回显 `strictly_disjoint=true`。结果为 `499/499` native accepted、`499/499` C7、`499/499` C5→C7 时间对、`499/499` 清理撤单获 EMT 接受，且无 producer/dispatch/callback/cleanup error 或 callback gap。

| 项目内路径（ms，499 个完整样本） | P50 | P95 | P99 | P100 |
| --- | ---: | ---: | ---: | ---: |
| C0→C0A admission | 1.298264 | 1.899029 | 1.970136 | 1.984988 |
| C0A→C0B native scheduler queue | 8.703410 | 16.182417 | 16.896292 | 17.060118 |
| C0A→C1 | 8.707957 | 16.187034 | 16.900748 | 17.064174 |
| PRE_NATIVE mmap commit | 0.005288 | 0.008553 | 0.070816 | 0.114593 |
| C0→C4A 提交内部总和 | 10.107033 | 18.118231 | 18.813016 | 19.050554 |
| C5→C5D SDK ingress 至 Python drain | 4.679643 | 9.536694 | 9.907072 | 10.014543 |
| C5A→C5B callback worker queue | 3.469743 | 8.107113 | 8.655088 | 9.126516 |
| C5→C7 callback 内部总和 | 9.032205 | 18.205792 | 18.452053 | 18.523480 |
| C4→C5 EMT 异步等待（不计入项目内） | 33.263946 | 46.967773 | 47.956802 | 48.016381 |
| C0→C7 直接测量 | 51.746996 | 66.746443 | 67.370221 | 67.378203 |

整批而非分位数的共同 C0 墙钟完成时间为：`C0→last C4 = 19.042312 ms`，`C0→last C4A = 19.050554 ms`，`C0→last C7 = 67.378203 ms`。由同一轮原始 499 条时间对重算的 callback drain 窗口为 **`first C5→last C7 = 56.406939 ms`**；它从最早 native callback ingress 开始，不等同于任意单笔的 C5→C7 分位数。相同 profile 的上一轮 C0→last C4/C4A 为 `23.531070/23.542197 ms`，本轮分别缩短 `4.488758/4.491643 ms`（约 `19.1%`）；C0A→C0B P95 亦从 `16.868956` 降至 `16.182417 ms`。但 C0→C0A P95 从 `1.799574` 增至 `1.899029 ms`，而 C5→C7 P95 从 `13.454293` 增至 `18.205792 ms`；后者主要来自本轮 C5→C5D 与 C5A→C5B 的回执窗口变化，未进行交错多轮 A/B，不能将单轮 C7 变化归因于 producer pin 或宣称回调性能回归/改善。完整 payload-free 原始时间对和 27 个分段直方图已随本轮隔离环境报告保存。

本轮仅通过“严格 CPU plan、499 全接纳/清理闭环及批量计时完整性”的代码与模拟盘证据；不改变 `DISARMED`，不证明 IRQ 隔离、硬实时、账户级多 session 长稳并行、故障恢复、profile 总验收或 `READY`。

## 2026-09-08：当前 Linux 分片 499 条性能定档（三轮）

Git revision `39b7c97` 在 Linux CPython 3.10、三个独立进程/EMT session 和严格
四角色 CPU plan 下完成三轮 499 条模拟盘压测。每轮均稳定分配 `167 / 166 / 166`，
三个 scope 的 native/producer/callback-ingress/callback-Journal CPU 分别为
`(180,181,182,183)`、`(184,185,186,187)`、`(188,189,190,191)`；各轮均为
499/499 native accepted、499/499 C7、499/499 完整 C5→C7 时间对、0 callback gap、
499/499 清理撤单受理，且所有 terminal callback 已释放 inflight credit（最终为 0）。

| 轮次 | C0→last C4A（ms） | C0→last C7（ms） | first C5→last C7（ms） | C0→C4A P95（ms） | C5→C7 P95（ms） | C0→C7 P95（ms） |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 49.314 | 62.344 | 52.324 | 25.768 | 17.334 | 61.885 |
| 2 | 29.727 | 73.518 | 64.436 | 21.647 | 10.608 | 70.852 |
| 3 | 22.698 | 61.323 | 48.870 | 18.650 | 22.405 | 59.798 |

逐轮分位数不能池化为单一分布；以下是三个“每轮分位数”的中位数，仅作当前基线
摘要：C0→C4A 为 P50/P95/P99/P100 `10.559 / 21.647 / 29.372 / 29.727 ms`；
C5→C7 为 `8.323 / 17.334 / 17.492 / 17.560 ms`；C0→C7 为
`54.257 / 61.885 / 62.336 / 62.344 ms`。C4→C5 仍是 EMT 异步等待，未计入两段
项目内部总延时；其三轮 P95 在 `37.816–48.299 ms`，是端到端总时延波动的主要来源。

因此当前定档不以单 session 499 条串行结果替代该 profile，也不以最低单轮成绩作为
SLA。后续若比较另一个架构，必须在相同 Linux、相同 499 总量、同一模拟盘窗口下，
同时报告两段项目内部延时和三项整批墙钟时间。

## 2026-09-08：P0 scope 进程 CPU 集合与运行时归因闭环（已验收）

Git revision `d50b873` 将每个 fork 后、SDK-owning scope 的初始线程在加载 EMT SDK
**之前**限制到自身四核集合；后续由该线程创建的 Python worker 和 EMT native callback
thread 继承此集合，角色线程仍继续固定到集合内的单一 core。聚合报告现在保留每个
scope 的实际 process CPU set 以及测量窗口外的 CPU 时间、上下文切换、缺页、迁核和
host IRQ 增量；这些并行窗口不会被错误相加为一个虚假的全局速率。该改动只影响压测
子进程，不修改主机全局 IRQ 亲和性或其他项目的调度。Git revision `d2f4fb7` 同时增加
服务器端从既有受控模拟 fixture 生成一次性本地 historical order-plan 的工具，使代码
仍通过 Git 拉取部署，不传输被忽略的交易计划文件；计划文件为 `0600`，报告只保存其
hash。

在相同 Linux/CPython 3.10/3 scope (`167/166/166`)/12 个互斥 CPU 的条件下完成三轮
499 条模拟盘压测。每轮均为 `499/499` native accepted、`499/499` C7、`499/499`
完整 C5→C7 时间对、0 callback gap、499/499 cleanup cancel accepted，且 cleanup
terminal callback complete、inflight remaining 为 0。

| 轮次 | C0→last C4A（ms） | C0→last C7（ms） | first C5→last C7（ms） | C0→C4A P95（ms） | C5→C7 P95（ms） | C4→C5 P95（ms，外部） |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| P0-1 | 21.385 | 59.931 | 50.249 | 17.483 | 10.486 | 39.537 |
| P0-2 | 21.114 | 61.002 | 50.757 | 17.179 | 17.689 | 37.634 |
| P0-3 | 18.537 | 1343.807 | 1332.958 | 17.276 | 6.805 | 1105.084 |

逐轮分位数不池化。三个“每轮分位数”的中位数为：`C0→C4A`
P50/P95/P99/P100 `9.554 / 17.276 / 20.777 / 21.114 ms`；`C5→C7` 为
`8.346 / 10.486 / 10.627 / 10.678 ms`；`C0→C7` 为
`49.723 / 60.331 / 60.994 / 61.002 ms`。相对上一三轮基线，提交内部 P95 从
`21.647` 降至 `17.276 ms`（约 `20.2%`），共同 C0 至最后 C4A 从 `29.727` 降至
`21.114 ms`（约 `29.0%`）。当前最大项目内提交段仍是 C0A→C0B native scheduler
queue，其中本轮三轮中位 P95 为 `15.327 ms`；mmap commit 和 SDK 同步调用仍为微秒级，
不是该 499 条全接纳 profile 的主导项。

P0-3 的约 1.1 s 尖峰发生在 **C4→C5 EMT 异步等待**，不计入项目内部两段总和。其
scope-2 测量窗口同时出现明显更高的 host IRQ 增量与迁核计数，而 C5→C7 项目内部反而
低于前两轮；该轮保留为外部回调/主机扰动证据，不能归因于 TradeCore hot path，也不能
用中位数掩盖。P0 因而证明了进程集合约束、证据采集和提交段改善；未证明 EMT callback
thread 已被固定到集合内某一个 core、IRQ 隔离、硬实时调度、更多 EMT session 的账户级
并行性或 `READY`。下一项应是固定 3 scope 条件下比较 callback wakeup/pipeline
(`threading.Event`/`eventfd`，4/8/12/16)，并保留每 scope 的 IRQ/迁核证据；扩展到
4 或 6 session 仍必须先证明 EMT 不会按账户聚合流控。

## 2026-09-08：P1 callback wakeup/pipeline A/B 与测量完整性门禁

在 P0 的同一 Linux 分片 profile 下，`threading.Event` 和 Linux `eventfd` 分别对
4/8/12/16 条 callback pipeline 候选运行 499 条全接纳闭环。所有候选均保持 native
accepted/C7/cleanup terminal 的 499/499 与 0 callback gap；比较仅使用 `C5→C7`
内部时间，不将柜台异步 `C4→C5` 等待作为 Python wakeup 的结果。

首轮扫描的 `eventfd/12` 曾给出 `C5→C7 P95 = 5.957 ms`，但交错复测后未复现。
三个“每轮分位数”的中位数如下（单位 ms）：

| 候选 | C5→C7 P50 | C5→C7 P95 | P99 | P100 | 结论 |
| --- | ---: | ---: | ---: | ---: | --- |
| `threading.Event / 8` | 5.561 | 10.221 | 10.394 | 10.456 | 保持默认 |
| `eventfd / 8` | 8.167 | 11.900 | 12.072 | 12.151 | 不切换 |
| `eventfd / 12` | 7.222 | 11.636 | 11.964 | 12.029 | 不切换 |

因此未改变 `threading.Event / 8` 的默认配置，也没有以最低单轮数值宣传优化。首轮
`eventfd / 4` 还暴露了整体 C5→C7 虽为 499 对、但 `C5A→C5B` 仅有 498 对的情况。
Git revision `6e70f2f` 已将此类缺口纳入完整验收：C5、C5D、C5E、C5A、C5B、route-state、
callback hash、callback mmap commit、C6 到 C7 的每个相邻子阶段都必须对全部 burst
样本具备单调时间对；少一对即不再标记 `complete`。这既使 eventfd/4 的那轮不具备候选
资格，也避免未来将无法归因的 callback 尾延时当成有效性能结论。

该门禁部署后，以保留候选 `threading.Event / 8` 新跑三轮；三轮均 `complete=true`，每一
个相邻 C5→C7 子阶段均为 `499/499` 单调时间对，且 native/C7/cleanup terminal 仍完整。
这三轮的 C5→C7 P50/P95/P99/P100 中位数为 `8.816 / 12.163 / 12.560 / 12.708 ms`。
它是当前契约的集成验证，不与先前交错 A/B 的不同主机扰动窗口混合为一个“更好”的统计值。

P1 的 callback 队列 A/B 已完成，结论是“保留当前实现”，而不是一次默认参数改写。下一
个可改变数量级的方向是增加独立 EMT session/process；该方向已在下述 P2 以四 scope
完成模拟盘闭环。它不构成账户级限流、长稳并行性或 `READY` 的证明。

## 2026-09-08：P2 四独立 session/process 分片（已验收，模拟盘）

Git revisions `6128463`、`89b027f`、`2c00535`、`bce0b3e` 与 `e79cd42` 增加并修正了
`--session-preflight-only`。这是一个与真实 burst 使用同一受控 execution adapter 的
**无订单**容量预检：每个 scope 在独立 Python 进程中建立实际 EMT adapter/session；父进程
按 scope 顺序、每个 scope 15 秒硬上限建立 session，同时保持已建立的 session 存活，避免把
同账户的登录串行误判为 session 数量上限。全部 scope ready 后，子进程先写入 payload-free
报告，再沿真实隔离 worker 的 `os._exit` 边界退出。预检明确没有 signal、submit、cancel 或
订单 query，且不把无法有界证明的 Python/SDK graceful teardown 伪装成通过条件。

四 scope 预检为 `4/4` adapter started、`4/4` isolated worker exit、`0` outbound mutation。
据此才启用四分片 499 条闭环：`125 / 125 / 125 / 124`。前三个 scope 延续
`(180,181,182,183)`、`(184,185,186,187)`、`(188,189,190,191)` 的
native/producer/callback-ingress/callback-Journal 四角色 CPU 计划；第四 scope 使用同 socket、
不同物理 core 的 `(176,177,178,179)`，16 个 CPU 无复用。每轮均启用 CPython GC 测量窗口
控制、`threading.Event / 8` callback pipeline，且保留原始 payload-free 时间对。

三轮每轮均为 `499/499` native accepted、`499/499` C7、`499/499` 完整 C5→C7 时间对、
`0` callback gap、`499/499` 单次清理撤单获接受，最终 inflight 为 `0`。逐轮分位数不池化；
下表是“三个每轮分位数”的中位数（单位 ms）：

| 路径 | P50 | P95 | P99 | P100 |
| --- | ---: | ---: | ---: | ---: |
| C0→C0A admission | 0.934389 | 1.321618 | 1.375539 | 1.388569 |
| C0A→C0B native scheduler queue | 6.340040 | 11.596437 | 12.640390 | 12.832748 |
| C1→C2 pre-native mmap path | 0.051166 | 0.081653 | 0.375362 | 0.801239 |
| **C0→C4A 提交内部总和** | **7.361038** | **13.051115** | **14.036370** | **14.335207** |
| **C5→C7 callback 内部总和** | **5.161987** | **9.758877** | **9.814721** | **9.854621** |
| C4→C5 EMT 异步等待（不计入项目内） | 35.078135 | 47.097646 | 48.230542 | 48.421238 |
| C0→C7 直接测量 | 52.839611 | 64.104783 | 64.557240 | 64.660034 |

对应整批墙钟时间的每轮中位数为：`C0→last C4A = 14.335207 ms`、
`C0→last C7 = 64.660034 ms`、`first C5→last C7 = 55.460535 ms`。相对 P0 三分片三轮
中位数，提交内部 P95 由 `17.276 ms` 降至 `13.051 ms`（约 `24.5%`），callback 内部 P95
由 `10.486 ms` 降至 `9.759 ms`（约 `6.9%`）。端到端 P95 没有作为 TradeCore 回归/改进
结论，因为它含 EMT 外部 C4→C5 等待；本轮的重点证据是 499 条全接纳下，项目内提交队列
从三分片的约 `166/167` 条降为四分片的 `124/125` 条后得到的数量级架构改善。

P2 通过的是“当前模拟盘窗口中四独立 EMT session 可以建立、共同 C0 接纳 499 条并完整
回收”的性能验收。尚未证明 EMT 在更长窗口或更高总速率下不会按账户聚合流控，也未证明
IRQ 隔离、硬实时、断电恢复、生产账户适用性或 `READY`；这些边界仍保持 `DISARMED`。

## 2026-09-08：EMT SDK receive/process CPU affinity A/B（可选，不设默认）

Git revisions `ba10c9b` 与 `4640fa1` 在现有四分片性能驱动中增加 EMT SDK 内部
receive/process 线程的可选 affinity 配置。两个 CPU 必须成对提供、非负且互异；
process-isolated 模式还要求其与 native worker、ingress producer、callback ingress、
callback Journal worker 的所有 CPU 跨 scope 全局不重复，否则驱动 fail-closed。每个 scope
在 EMT login 成功后调用 SDK 的 `setCpuAffinity`，并将请求与实际完成状态写入无 payload
预检报告；未配置参数时保持先前行为，既不调用 SDK affinity，也不改变默认 profile。

同日无订单预检证明 `4/4` scope 均已启动 adapter、均已完成 SDK affinity 调用、所有六类
CPU 角色严格互斥，且预检没有 signal、订单、撤单或订单查询。随后在同一 Linux、CPython
3.10、四个独立 EMT session、499 条共同 C0、`threading.Event / 8`、GC 测量窗口控制和
相同受控模拟 fixture 下，分别进行了三轮“不设置 SDK affinity”的 control 与三轮“设置
receive/process SDK affinity”的 candidate。每一轮都是 `499/499` native accepted、
`499/499` C7、`499/499` 完整 C5→C7 时间对、0 callback gap、`499/499` 清理撤单获接受，
terminal 后 inflight 为 0。

逐轮分位数不池化。下表为三轮“每轮分位数”的中位数，单位为 ms；C0→C7 含 EMT 外部
C4→C5 等待，只作完整性观察，不作为本优化的选择依据。

| 路径 | SDK affinity 关闭 | SDK affinity 开启 | 差值（开启－关闭） |
| --- | ---: | ---: | ---: |
| C0→C4A P50 / P95 / P99 / P100 | 7.403885 / 13.338272 / 14.080277 / 14.335581 | 7.820463 / 13.643905 / 14.762516 / 15.102117 | +0.416578 / +0.305633 / +0.682239 / +0.766536 |
| C5→C7 P50 / P95 / P99 / P100 | 6.529391 / 9.856120 / 11.614232 / 11.777317 | 4.911291 / 8.467004 / 8.625192 / 8.690380 | -1.618100 / -1.389116 / -2.989040 / -3.086937 |
| C0→C7 P50 / P95 / P99 / P100（含外部） | 44.768550 / 60.963754 / 61.703586 / 61.713721 | 49.862897 / 62.834801 / 63.524247 / 63.533230 | +5.094347 / +1.871047 / +1.820661 / +1.819509 |

整批墙钟时间的三轮中位数为：未设置时 `C0→last C4A = 14.335581 ms`、
`C0→last C7 = 61.713721 ms`、`first C5→last C7 = 51.779504 ms`；设置时分别为
`15.102117 ms`、`63.533230 ms`、`51.609481 ms`。细分观察中，SDK affinity candidate 的
`C5→C5D` P95 为 `5.107937 ms`（control 为 `5.701507 ms`），`C5A→C5B` P95 为
`3.983316 ms`（control 为 `4.800002 ms`）；但提交主导段 C0A→C0B P95 从 `11.638508 ms`
变为 `12.004962 ms`。这说明候选在本窗口的 callback 调度上存在改善信号，却没有同时改善
499 条全接纳的提交尾延时。

本 A/B 为同日顺序运行，尚非随机交错试验；并且 C4→C5 P95 同时从 `42.349084 ms` 变为
`45.493649 ms`，因此绝不将 C0→C7 的变化归因于 TradeCore。基于两段项目内部指标的取舍，
结论是：**保留实现与预检证明，但不写入默认性能 profile，也不宣称整体 499 条优化通过。**
只有 callback 延时优先、且在固定主机上完成后续交错复验后，才可显式启用该可选参数；当前
四分片默认仍以未设置 SDK affinity 的 profile 为基线。

## 2026-09-08：native scheduler 空→非空唤醒合并（实现通过；非数量级优化）

在四个 scope 的当前 499 条 profile 中，`C0A→C0B` 仍主要反映每个单 caller EMT lane
顺序服务约 `124/125` 笔请求的时间。为了去除不改变状态的 Python 开销，Git revision
`1d6550a` 将 scheduler 的 `Condition.notify()` 从“每次入队”改为“两个队列均为空时的
首次入队”。同一 session 仍只有一个 native consumer；native-inflight credit、容量/字节
限制、deadline、队列 FIFO 以及 cancel-first dequeue 均未改变。因为 `Condition.wait()`
与该检查共用同一把锁且原子释放，不会丢失空→非空唤醒。

本修改前还进行了一次第五 scope 的**无订单**容量预检：既有四个 scope 均已启动，但第五个
未在有界预检内成为 ready，因此没有伪造 scope、没有提交订单，也没有将第五 scope 混入
后续压测。当前可证明的并行度仍为四个独立 EMT session/process。

部署后在相同 Linux/CPython 3.10、四 scope、共同 C0、GC 测量窗口控制与相同受控模拟
fixture 下完成三轮 499 条模拟盘闭环。每轮均为 `499/499` native accepted、`499/499` C7、
`499/499` 完整 C5→C7 时间对、0 callback gap、`499/499` 清理撤单获接受且 terminal
inflight 为 0。每轮的四个 scope 分别只记录 `1` 次 dispatch wake notification，合计为
`4`；旧实现会在每次成功入队时通知，因此同一全接纳 499 条 burst 会尝试 `499` 次通知。

逐轮分位数不池化。此次三轮“每轮分位数”的中位数（ms）为：`C0→C4A`
P50/P95/P99/P100 `7.362390 / 13.378196 / 13.912184 / 14.266568`；`C5→C7` 为
`4.448059 / 11.180975 / 11.393270 / 11.457405`；`C0→C7`（含外部 C4→C5）为
`46.312806 / 64.212980 / 65.775952 / 65.877482`。对应整批墙钟中位数为
`C0→last C4A = 14.266568 ms`、`C0→last C7 = 65.877482 ms`、
`first C5→last C7 = 56.814498 ms`。

相对同日未启用 SDK affinity 的三轮 control，提交 P95 从 `13.338272` 变为
`13.378196 ms`，C0A→C0B P95 从 `11.638508` 变为 `11.881811 ms`，未达到可宣称的
499 条尾延时改善；callback P95 的变化同样不能归因于本修改，因为运行非随机交错且受
EMT/主机调度窗口影响。故该改动只作为已验证的无效唤醒消除保留，不改变性能 SLA 或
四分片定档。若要继续压低 C0A→C0B 的数量级，必须获得柜台可用的额外、可审计 scope，
或取得同 session 并发调用的长期稳定证明；不能靠移动 C0/C0A 时间边界、无界队列或伪造
第五 session 来制造结果。

## 2026-09-08：热态单笔 1-session / 4-session 同口径基线（模拟盘，通过）

Git revisions `5c10577`、`33aa64f`、`66245ec`、`61e79a3`、`c2cbcbc` 与 `3dc8f09`
在同一驱动中增加 `--hot-single-order-samples`。它不是第三种 burst 宽度：每个 scope 的
EMT adapter/session、callback worker 与单 native caller 都先完成预热；父进程随后只发放**一个**
release token，子进程仅在该笔从 C0 到 C7、一次撤单受理与终态 callback 闭合后确认 token，
父进程才发出下一笔。因此账户侧始终没有两笔 submit/cancel 链路重叠，父子进程的协调 IPC
不计入 C0，C0 固定为 owning scope 真正进入 TradeCore scheduler 的时刻。

两组均使用同一受控模拟盘 order fixture、`threading.Event / 8` callback pipeline、GC 测量窗口
控制和 payload-free 原始时间对。每笔 C7 后另等待至少 `1.1 s` 再撤单；这是与 499 验收一致的
柜台稳定性清理规则，完全位于 C0→C7 之外，不能计入单笔延时。两组均为 `100/100`
native accepted、`100/100` C7、`100/100` C5→C7 完整时间对、`0` callback gap、
`100/100` 撤单受理、终态 inflight `0`，且 producer/dispatch/callback/cleanup error 均为零。

| 路径（ms） | 1 session P50 / P95 / P99 / P100 | 4 session P50 / P95 / P99 / P100 |
| --- | ---: | ---: |
| C0→C0A admission | 0.053670 / 0.061122 / 0.069144 / 0.069605 | 0.065388 / 0.083145 / 0.106991 / 0.135824 |
| C0A→C0B native scheduler queue | 0.106480 / 0.154262 / 0.163155 / 0.167592 | 0.073520 / 0.105919 / 0.164056 / 1.123526 |
| C0A→C1 native-ready | 0.140551 / 0.193501 / 0.203165 / 0.204537 | 0.112569 / 0.148533 / 0.206360 / 1.155624 |
| **C0→C4A 项目内提交** | **0.536425 / 0.601613 / 0.609785 / 0.633170** | **0.581312 / 0.637417 / 0.873701 / 1.524057** |
| **C5→C7 项目内 callback** | **0.415934 / 0.447401 / 0.659921 / 3.381456** | **0.479610 / 0.525699 / 0.715594 / 1.810738** |
| C4→C5 EMT 异步等待（外部） | 8.849284 / 9.147151 / 9.364106 / 9.609644 | 10.016371 / 10.982773 / 11.080089 / 11.499088 |
| C0→C7 直接测量（含 EMT 外部等待） | 9.740953 / 10.067212 / 10.485910 / 12.513023 | 11.066169 / 12.091907 / 12.591527 / 13.321853 |

这项实验回答的是“常驻四分片会不会让空闲单笔顺序经过四条 lane”：**不会**。每笔只走其
稳定归属的一个 scope，热态 `C0A→C0B` 队列 P95 均低于 `0.16 ms`；不存在四条 lane 串行相加。
但在全局严格单笔下，四个常驻 SDK process/session 和主机调度资源也没有带来低延时收益：
本窗口中 C0→C4A P95 比单 session 高 `0.035804 ms`，C5→C7 P95 高 `0.078298 ms`。C0→C7
的 P95 差异不可归因于 TradeCore，因为其中 `C4→C5` EMT 外部等待增加了 `1.835622 ms`。
四分片仍应作为 499 突发时降低每条 native lane 排队深度的架构，而非为低并发动态择优路由；
后者会破坏稳定 ownership、回调归属和恢复证据，且本基线没有显示其必要性。

## 保留的边界与后续验收

异步 SSD archive、PostgreSQL read model 和任何未来 projector 只能消费已经确认的 mmap position，不能反向确认、改写或阻塞 C0→C7。当前尚缺完整的 merge manifest、profile 专属恢复 reader、崩溃矩阵和固定环境下的容量验收；其门禁以 [`测试与验收规范`](../01-测试与验收规范.md) 第 5.1 节为准。

历史报告和实施证据可保留为不可执行审计材料，但不得作为当前 profile 的依赖、部署说明或性能结论。
