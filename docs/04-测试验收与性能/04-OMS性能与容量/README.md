# OMS 性能与容量

本目录保存 OMS/EMS 旁路设计、内部穿透性能实测和容量上限结论；它们与 EMT 的 C0–C7 真实柜台链路分开记录，避免将 OMS-only 吞吐误报为 EMT 委托能力。

| 文档 | 内容 |
| --- | --- |
| [Python OMS/EMS 综合热路径优化方案](./01-Python-OMS-EMS综合热路径优化方案.md) | 定义纯 Python 热路径、旁路原则与 499 burst 验收边界。 |
| [OMS/EMS 旁路能力优化实施方案](./02-OMS-EMS旁路能力优化实施方案.md) | Journal replay、状态机、EMS control plane、snapshot 及 direct-write 的实施计划。 |
| [OMS 内部路径 TPS/QPS 实测报告](./03-OMS内部路径TPS-QPS实测报告-2026-09-08.md) | W0–W7 的 HTTP/OMS 负载、测量口径和实测结果。 |
| [OMS 持续饱和上限实测报告](./04-OMS持续饱和上限实测报告-2026-09-08.md) | 热态单笔、持续写入/查询混合负载和饱和上限的证据。 |
| [BenchmarkStandardV1 复验报告（2026-09-11）](./05-BenchmarkStandardV1复验报告-2026-09-11.md) | 当前三轮 `TC-EMT-499`、热态单笔和 OMS W7 并行复验；明确保留 R1 权威恢复的厂商回调阻塞边界。 |
| [R1 统一验收与性能复验报告（2026-09-11）](./06-R1统一验收与性能复验报告-2026-09-11.md) | R1-A1--A6、Hot100、三轮 499 与 OMS W7 的同一冻结 revision 证据汇总；明确该结论仍为 `RECOVERED_DISARMED`，不是 `READY`。 |
| [EMS 与策略消费端统一性能压测报告（2026-09-12）](./07-EMS与策略消费端统一性能压测报告-2026-09-12.md) | 历史窗口：E0--E5、S0--S5 已通过；当日 EMT callback `0/499` 令联合 W7 失败。该外部投递阻断已由 2026-09-14 的有效窗口重新验证，不代表当前 callback 状态。 |
| [2026-09-18 控制面与联合验收记录](./10-2026-09-18控制面与联合验收记录.md) | 当前 revision 的控制面 short-lease F2、3×499/Hot100、OMS/EMS/Strategy W7 和 EMT query/binding 复核；明确保留本轮 W7 cleanup terminal 阻断。 |
| [本地热路径饱和容量实测报告（2026-09-12）](./08-本地热路径饱和容量实测报告-2026-09-12.md) | H1 策略信号 11k TPS、H2 checkpoint event 14k TPS、H3 snapshot 10k QPS 的三轮认证容量；明确 H2/H3 硬上限尚需独立 load generator 证明。 |
| [V1.1 真实 Callback Direct-Write A/B 验收记录（2026-09-14）](./09-V11-真实Callback-DirectWrite-A-B-验收-2026-09-14.md) | 三组交错真实 499-burst 均完整闭环；Direct 的 callback 交接虽缩短，但 `C5→C7` P95/P99 不优于 Control，故默认路径保持 Control。 |
| [真实 UDS Consumer V2 压测与验收标准](./11-真实UDS-Consumer-V2压测与验收标准.md) | 面向独立开发者的真实 Linux UDS 基线：U0–U7、2,000 TPS/5,000 QPS/5,000 event TPS、背压和脱敏 evidence；按 profile 分项验收，不能将单流通过视为混合通过。 |
| [服务器真实 UDS 部署与压测方交接](./12-服务器真实UDS部署与压测方交接.md) | 独立 UDS 服务部署、同机调用与受控 reset。 |
| [UDS S1 吞吐瓶颈修复与部署复测](./13-UDS-S1吞吐瓶颈修复与部署复测-2026-09-29.md) | S1 修复及对应部署复测证据。 |
| [PollEvents 签名分片游标与 S2 联调说明](./14-PollEvents签名分片游标与UDS-S2-5K联调说明.md) | 正式分片字段、持久 checkpoint、逐请求 trace 与证据校验工具。 |
| [UDS 混合负载快照隔离优化与验收方案](./15-UDS混合负载快照隔离优化与验收方案.md) | S5 大快照阻塞的实测依据、S5-A 至 S5-E 实施包、分页与全量口径、集中验收、部署回滚；新增功能待实现、待验收。 |
| [固定版本消费者黑盒全面压测报告（2026-10-17）](./18-固定版本消费者黑盒全面压测报告-2026-10-17.md) | 固定 4,096 首帧容量版本的 HOT、混合负载、5,000 突发与有界过载基线。 |
| [Consumer UDS 首帧接入改造与完整门禁报告（2026-10-19）](./19-Consumer-UDS首帧接入改造与完整门禁报告-2026-10-19.md) | 自适应首帧模型、A/B 选型和 R1–R3 全部门禁；5,000 全接纳已通过，`<1 s` 与三轮混合性能仍 fail-closed。 |
| [Consumer UDS 2,000 TPS/QPS 三轮验收报告（2026-10-19）](./20-Consumer-UDS-2000TPS-QPS三轮验收报告-2026-10-19.md) | 2026-10-19 指定版本的历史三轮结果；不可替代当前 wheel 和当前拓扑的完整验收。 |
| [Consumer UDS 长运行优化实施与验收方案](./21-Consumer-UDS长运行优化实施与验收方案.md) | 待实施L0–L7：分段Journal、checkpoint/幂等/cursor恢复、有界状态、trace与GC；强制低频/高频同窗、反复升降压及背景突发，执行2h/24h/72h不reset验收。 |
| [两小时高低并发混合压测前置检查（2026-10-19）](./22-两小时高低并发混合压测前置检查-2026-10-19.md) | BLOCKED_PREFLIGHT：长运行代码/发生器未实现，单文件fact容量不足；正式两小时压测未启动，服务保持正常。 |
| [长运行功能包实施与验收记录](./23-长运行功能包实施与验收记录.md) | L0–L6子包、正式目录的手动长运行qualification入口、重启恢复与三轮2K混合失败证据；生产LONG_RUNNING/READY仍未开放。 |
| [Consumer UDS 十二小时长运行压测与验收方案](./24-Consumer-UDS十二小时长运行压测与验收方案.md) | 12小时MIX共72周期，另列持续2K轨道；规定容量、SLO、资源、停止条件和证据，开跑仍须完成长期runtime前置门禁。 |
| [Consumer UDS 长运行瓶颈优化实施与逐步验收方案](./25-Consumer-UDS长运行瓶颈优化实施与逐步验收方案.md) | 待实施S0–S8：测量/发生器、首帧调度、固定CPU角色、读连接复用、投影批处理、条件WAL、签名分片消费、完整退出及正式组合复验；每步含验收、证据与回滚。 |
| [HIGH 混合波动归因与代码优化逐步验收方案](./26-HIGH混合波动归因与代码优化逐步验收方案.md) | H0–H7：统一证据、Journal 维护归因与条件优化、单 owner 前排队、Snapshot/PollEvents、资源隔离及集中验收部署；状态为待实施。 |
| [生产版本与性能基线（2026-10-28）](./27-生产版本与性能基线-2026-10-28.md) | 已安装 wheel、源码 checkout 和候选版本区分；历史参考、正式 unit 未实测边界及升级条件。 |
| [TradeCore 与 HyperPerf 联调开发交接文档](./28-TradeCore与HyperPerf联调开发交接文档.md) | 服务器与实际 UDS 身份、HTTP/UDS 合同、平台待补能力、进程资源监控、初始化与分阶段验收；不恢复暂停的开发或授权发压。 |

执行入口和结果 schema 位于 [`tools/oms_load_platform/`](../../../tools/oms_load_platform/README.md)；EMT 原生接口性能合同见[上级 EMT 接口性能目录](../03-EMT接口性能/README.md)。
