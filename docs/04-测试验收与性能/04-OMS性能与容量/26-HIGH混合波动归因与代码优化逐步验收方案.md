# HIGH 混合波动归因、代码修改与逐步验收方案

状态：`DESIGNED`。本文是待实施方案，不代表代码已修改、测试通过或生产部署完成。它补充 [25 号长运行瓶颈优化方案](./25-Consumer-UDS长运行瓶颈优化实施与逐步验收方案.md)，不替代其中安全合同、资源和正式验收要求。涉及生产发布时同时遵循 [统一性能压测标准](../05-统一性能压测标准.md)。

## 1. 目标与范围

在保持单账户单 OMS writer、事实顺序与恢复语义的前提下，降低 HIGH 混合负载中 `SubmitSignal`、`ReadOrderSnapshotPage` 和 `PollEvents` 的共同波动，尤其是 Submit 在 frontend 发出后、owner 收到前的长尾；验证并减少 Journal 后台维护对其他路径的干扰。不得仅将排队从一个位置转移到另一个位置，也不得只优化诊断分段而忽略消费者端总延时。

本方案使用 Linux、项目 Python 3.10、真实 UDS 消费者接口以及 no-Adapter Mock；不调用 EMT/QMT，不将本地 TPS、QPS 当作柜台能力。固定资源版本仍使用固定角色和启动前冻结的容量；不引入运行时扩容或混合自适应策略。正式 EMT 500 笔/秒与既定安全限制另行适用。

## 2. 基线事实与待证假设

2026-10-27 在运行代码 `6aca2c2` 的三个 fresh candidate 上执行 60 秒诊断，每轮计划 120,000 Submit 与 120,000 Snapshot，最终均成功；R2 有 85 个 Submit 窗后完成。三轮 HIGH **0/3 通过原门禁**。各轮最终 240,800 个 Mock 事件与 source/lifecycle 闭合；不等于事件均在窗口内消费或已通过生产资格。

| 指标，毫秒 | R1 | R2 | R3 |
| --- | ---: | ---: | ---: |
| Submit scheduled P95 / P99 | 32.293 / 65.110 | 52.953 / 119.669 | 21.823 / 61.571 |
| Snapshot scheduled P95 / P99 | 6.099 / 30.638 | 7.172 / 42.861 | 5.014 / 8.262 |
| PollEvents call → FULL progress P95 / P99 | 11.854 / 22.168 | 12.575 / 25.428 | 10.830 / 20.742 |

证据源：`/home/xuanling/tradecore-fact-keys-high60-20261027T092320Z/R1` 至 `R3`，候选与 source-join 路径由各轮 manifest/lifecycle 解析。证据可能随保留策略变化；执行前必须核验文件仍存在、closed、完整及 hash。缺失时标记 `EVIDENCE_UNAVAILABLE`，不能按历史数字重新签发验收。

现有分析支持以下方向，但不证明唯一根因：

1. R2 第 43 秒的 2,000 个 Submit 客户端请求与 owner 记录逐笔关联：客户端调用→owner 收到 P99 为 199.655 ms，owner 收到→完成 P99 为 4.339 ms。不同分位数不可相减；该轮缺少全链路 frontend/broker 逐笔配对，不能直接锁定某个队列。
2. 约每 8 秒出现候选进程约 256 MiB 写入增量及部分延时峰值，与 65,536 × 4 KiB segment 的尺寸及维护行为吻合。写入进程与具体 prepare/seal 的身份尚待证明；1 秒采样和相同周期只能说明关联。
3. R2 第 59 秒 PollEvents call→response P95 为 38.796 ms，response→FULL progress P95 为 132.010 ms；服务端与消费者 FULL checkpoint 都可能有贡献，不同分位不可相加。
4. 后段触发 memory.high，但早期尖峰发生时未触发；不能把所有波动归因于内存。最后一秒生成器排队亦有明显增长，不能把全部 scheduled 延迟归于服务端。采样的 CPU 等待为线程累计，不是单笔等待。

上次 owner 固定字段缓存候选默认关闭；压小 handoff 窗口、调整 GIL、迁核及 Mock 批次等试验有失败或无稳定收益证据，不作为本方案默认优化。除非新证据支持并重新 A/B，不重复采用。

## 3. 不可变更的安全与测试合同

- 保留单 writer、Journal-first、订单/事实顺序、完整 batch、commit-last、CRC/hash、原水位推进、binding/scope、鉴权、风控和幂等；任何可见成功仍满足既有持久化与完整性条件。
- 投影和消费者 checkpoint 保持既定 FULL 语义，不使用 `synchronous=OFF`、关闭必要刷盘、提前 ACK、降低快照一致性或不安全自动重试换取性能。未知提交结果仍为未知，按原合同恢复和对账。
- 有界连接、队列、credit、字节、内存、磁盘、FD 和进程；不通过无界排队、丢弃请求、缩短测量窗、漏记失败、扩大门槛掩盖问题。原 128 总 credit、队列容量和 6 GiB 上限不因本文自动调整。
- trace 默认关闭或显式启用；任何诊断不得在提交热路径同步写文件、刷盘或泄露账户、订单、token、正文。buffer 满、缺页、丢失、跨代、时钟不匹配要显式报错或标记证据不完整。
- 用同版本合同、同窗口、同资源和同源工作负载 A/B；各新轮使用新的进程、generation、Journal namespace 和唯一请求空间。基线和候选运行代码、构建 wheel、配置、测试工具及其 hash 必须冻结并记录。
- 控制测试只使用本项目批准资源，不重启服务器或改变其他项目；不可仅凭 `AllowedCPUs` 宣称 CPU 独占。涉及宿主全局 sysctl、IRQ、I/O scheduler 或现有生产服务变更必须先核对权限、影响与回退，未经另行批准不修改。
- 阶段状态使用 `DESIGNED`、`IMPLEMENTED_NOT_ACCEPTED`、`MODULE_ACCEPTED`、`COMBINATION_ACCEPTED`、`FAILED` 或 `NOT_ADOPTED_WITH_EVIDENCE`；跳过和缺失不算通过。每步完成修改及对应验收后，提交并 push 当前任务分支、核验远端 commit；失败实验可提交但必须标明失败且默认关闭。

## 4. 阶段顺序与交付

| 阶段 | 目的 | 完成后进入下一步的条件 |
| --- | --- | --- |
| H0 | 冻结实验与完整证据，复算共同时间轴 | 证据校验、时钟和发生器自检通过 |
| H1 | Journal 维护、设备 I/O 与请求逐次关联 | 能明确关联或排除，证据缺失不可得出因果 |
| H2 | 条件性平滑维护工作 | 恢复语义、故障与配对 A/B 通过；无收益不采用 |
| H3 | owner 前逐笔链路与单 owner 消费能力 | 完整配对、公平、有界及 A/B 通过 |
| H4 | Snapshot 与 PollEvents 分段及隔离 | 响应合同、checkpoint 语义和 A/B 通过 |
| H5 | 资源/发生器控制及组合候选冻结 | 每角色资源、调度与证据完整；不干扰其他项目 |
| H6 | 集中 LOW、HOT、故障与 HIGH 三轮 | 全部原门禁通过后才可申请正式部署 |
| H7 | Git 部署及上线后复验 | 实际运行 wheel/配置一致、健康与回滚证据完整 |

每阶段先完成代码、单元/契约/故障模块验收，再运行本阶段限定对照；统一性能、压力和完整故障矩阵在组合功能包完成后集中验收。某个可选实验没有通过时保留失败证据并恢复原实现，不能阻止其他独立证据建设，但不得将未通过实验合入最终组合。

## 5. H0：冻结基线、账本和共同时间轴

### 实施

1. 沿用 `tools/performance/consumer_uds_candidate_probe.py`、`consumer_uds_candidate_shards.py`、`consumer_uds_rolling_events_probe.py` 及 `perfbench/longrun_evidence.py`，冻结生成器、负载、种子、事件模型、预热与 drain；每条分别保留 planned、call、send、response、validated、终态及 request digest。记录未发、拒绝、超时、未知提交、窗口内与窗后完成，严格守恒。
2. 使用同一主机 `monotonic_ns` 和 clock identity。记录每次采样的实际起止，而不是假设样本严格为整秒；用同一时钟逐请求关联和分桶，并报告该桶样本数。采样是区间估计而非连续 trace；有重叠时间段不代表直接因果。
3. 验证原始 segment hash、manifest、sequence、generation、生命周期、source/consumer checkpoint 和最终 LSN。敏感身份只保留不可逆 digest；不得输出凭据、账户、标的或订单正文。
4. 使用受控可复现 responder 验证发生器；单独报告生成器计划→call 分布。预热、测量、final drain 分开，drain 不得算入窗口吞吐。历史证据和新运行分开归档。

### 验收

- 已知计划数等于各种互斥最终分类之和；所有成功样本时间单调、唯一且可配对。截断、错 hash、重复、跨代、不同 clock identity、缺窗口和 trace drop 被拒绝，不能默认 0 或通过。
- 已发请求须明确成功、拒绝、超时或提交结果未知；构造断连和客户端超时，未知不得自动重发变新单。
- 发生器对照在固定资源下同时发放 2,000 Submit/s 与 2,000 Snapshot/s，漏发 0、计划→call P99 ≤10 ms；此项只证明发生器能力。真实 TradeCore 轮次仍须独立满足全部账本和时延标准。
- 输出分阶段 paired raw、完整 hash/工具版本、采样缺项及资源/隔离声明。缺少必需数据为 `INCOMPLETE`，不停止后续安全的证据补齐，不将其视作成功。

## 6. H1：分段维护与 I/O 因果证据

### 实施

修改点：`oms/segmented_fact_journal.py`、`persistence/process_segment_sealer.py`、`consumer/request_trace_v2.py`，以及 `tools/performance/sample_consumer_resources.py` 和离线分析器。

1. 在维护进程记录有界事件：generation、segment index、进程/角色、operation、start/end、预分配大小、预触页数、状态及错误分类；拆分 prepare 的 `ftruncate/fallocate`、prefault、`mmap.flush`、`fsync`、manifest 发布，以及 seal 校验/flush/fsync。只在维护侧写诊断队列；不改变原 manifest 发布次序。
2. 记录 segment 激活/切换与备用水位，关联到受影响窗口；记录 writer/maintenance 独立身份，证明某进程及字节变化属于哪个操作，不能根据尺寸推断进程身份。
3. 在有权限且安全的条件下采集 cgroup 设备 I/O、CPU、调度、memory.high/PSI、page fault、进程写入和块设备延迟；工具能力或权限不足记 `NOT_MEASURED`。如需 eBPF、task delay accounting 或系统范围配置，先进行独立影响评估和恢复，不将修改视为默认授权。
4. 将维护事件与 Submit、Snapshot、PollEvents 及消费者 FULL 阶段按同一时钟对齐，给出每个异常的数量、同窗口对照及数据覆盖率；分别保留维护发生时和未发生时的样本，不只展示最差秒。

### 验收

- 已知操作 fixture 能严格对应一次开始和完成；异常/进程被杀/trace 满/时钟不一致均能识别，无伪造结束；无敏感内容。
- 完整诊断轮达到 100% 已记录的 segment 操作身份匹配，所有选定 Submit 具备客户端与 frontend/broker/owner 配对；拒绝缺失、重复和跨代。操作的真实数量以 manifest 和原始 trace 核对。
- 与关闭诊断的相同负载交错 A/B 三组，HOT P95 相对增幅 ≤5%，同时满足 HOT 绝对门槛；不满足则优化采集或标为有扰动诊断，不用它认证性能。
- 给出「支持」「不支持」或「证据不足」结论；仅相关不能写成根因。证明不了时直接进入 H3/H4 的独立归因，不以猜测实施 H2。

## 7. H2：条件性平滑 Journal 维护

### 实施

前提是 H1 证明维护操作与延迟有可复现关联且不会破坏可靠性。优先在现有 `segmented_fact_journal.py` 与 `process_segment_sealer.py` 旁路线中实现默认关闭、可回滚候选：

1. 评估在安全的备用水位内提前准备，比较低负载空隙分阶段预触页或限制每次维护工作量；不得让一个尚未完成分配、预触页（若合同要求）、flush/fsync 与 manifest 验证的 segment 进入 ready 队列。
2. 维护速率限制必须有界，既不能饿死新 segment，也不能在高负载下依赖不可保证的空闲时间；预测库存不足时按原规则停止新写入或 fail-closed，不能覆盖、跳过或重排事实。
3. 仅对本项目维护进程比较固定 CPU/I/O 调度及写入节奏；不改变热路径每笔同步合同、不减少事实、不修改全局系统策略。若涉及 segment 尺寸、预算或持久化合同变更，先单独评审后再实验。

### 验收

- 单元覆盖启动、ready 水位、切换、空间不足、短写、flush/fsync 失败、维护超时、队列满及受控退出；所有失败可观察且不得发布未验证 segment。
- Linux 真进程故障矩阵覆盖 prepare/manifest/activation/append/seal 前后 SIGKILL、重启和磁盘故障注入；与未优化版本对比 Journal 事实、顺序、最后完整 batch、去重与投影，半帧不可被误认成功，任何已确认事实不得无证据消失。
- 两侧各至少三轮交错 60 秒诊断，冻结资源和 trace 配置，报告维护耗时、备用水位、I/O、三类端到端分位及每轮所有失败。对照不能只挑最好轮；不改善共同尾部、出现资源/可靠性退化或 LOW/HOT 退化则 `NOT_ADOPTED_WITH_EVIDENCE`。
- 本阶段不单独宣称 HIGH 通过；最终由 H6 全组合重新验收。

## 8. H3：owner 前排队与单 owner 消费能力

### 实施

修改点：`consumer/frontend_owner_broker.py`、`spawned_owner_brokers.py`、`spawned_frontend_worker.py`、`process_oms_owner.py`、`broker_diagnostic_trace.py` 和 `tools/performance/analyze_broker_handoff.py`。

1. 为计划、客户端调用/发送、frontend 收帧/offer、broker receive/offer/最后一次 send 前后、owner receive/start/finish、响应与客户端校验建立一对一脱敏 request ID、generation 和 serial。并发 send 返回与另一 CPU receive 可能交错，保留上下界，禁止伪造精确网络耗时。
2. 同步记录每 frontend 有界排队深度、最老请求年龄、共享 credit、owner 在途数、每 turn 处理数量/时间和 READ 服务预算。统计每 frontend 饥饿与跨请求公平性；禁止用总体平均掩盖单 frontend 长尾。
3. 依据新 CPU profile、火焰图或逐段墙钟/线程 CPU 证据，优先减少单 owner 的重复解析、对象构建、不可变验证和跨进程交接；仅可缓存经完整键值与 generation/profile 校验的不变量，严格限定容量与生命周期。可变订单状态、授权、风控、幂等和恢复验证不能跳过。
4. 对时间预算与公平轮转仅做可回滚、启动前固定的候选；保留单 writer、同账户顺序、原 128 credit 与 READ 最低服务预算。不得只缩小 handoff 队列让等待转移上游。

### 验收

- 不同 frontend、并发、队列满、EOF、部分写、未知结果、重放/重复、owner 进程死亡/重启均按原合同处理；相同账户顺序及唯一提交、credit 归还和全部上限可证。
- 全部目标请求的各侧 trace 完整且一对一；数据缺失时不得宣称指定节点改善。比较 frontend→owner、客户端 scheduled、每 frontend 最老等待和端到端延迟，不能只比较内部段或把总体分位相减。
- 候选与基线交错至少三组，逐轮报告 Submit P50/P95/P99/P100、吞吐、队列峰值和 Snapshot/PollEvents 对照。对同合同 LOW/HOT 无退化、故障等价且最终达到 H6 原门槛才可启用；不能凭短轮或单轮最优采用。

## 9. H4：Snapshot 与 PollEvents 分段和隔离

### 实施

1. Snapshot 记录 frontend 入队、read plane 排队、generation/水位检查、查询/序列化、发送和客户端校验；固定分页和快照合同，记录请求页大小与响应字节，避免比较不同数据集。
2. PollEvents 单笔保留 call→response、response→checkpoint progress，并在 checkpoint 侧记录验证、SQL 准备、FULL commit 前后及提交确认。仅以同一条请求的时间求阶段值；完整源事件→FULL 另做逐事件配对，不等同单次 RPC。
3. 若 H1/H3 证实 READ 与写入/维护竞争，才在现有 `process_owner_read_plane.py`、`frontend_read_broker.py` 和 `perfbench/consumer_event_checkpoint.py` 中做有界、公平的调度或批量开销优化；分片、签名 cursor、读水位、顺序、唯一性及 FULL 语义不变。
4. 先区分 TradeCore 服务端与测试消费者 checkpoint，独立归属优化；不得通过关闭 checkpoint、只统计非空页、把事件窗后 drain 写作窗内完成或减少订阅事件过关。

### 验收

- 同一冻结数据集的分页、snapshot generation、水位、权限和响应内容逐条等价；过期 cursor、跨 scope、乱序、重复、空页、范围边界、读面崩溃和恢复全部 fail-closed 或符合合同。
- FULL 前/中/后崩溃，checkpoint 重启后无跳号、无静默遗漏；重复重放被正确识别。输出 source→FULL 的完整事件账本及窗口内/窗后分类。
- 给出 RPC 与 checkpoint 阶段各自逐笔分布和 P50/P95/P99/P100；三组交错 A/B 中同时检查 Submit、Snapshot、PollEvents，不能用一类改善换取其他门槛失败。正式通过仍以 H6 原合同判断。

## 10. H5：资源隔离与组合冻结

### 实施与验收

1. 冻结 cgroup、各进程/TID 的 CPU 亲和性、SMT 关系、内存高水位/上限、I/O、生成器和 sampler CPU；核验实际运行归属和服务器上其他服务无资源集合冲突。不能声称未验证的物理核或 IRQ 独占。
2. 用时间序列关联 CPU PSI、runqueue、上下文切换、page fault、memory.high、设备 I/O 与各阶段 latency；记录采样周期、缺失和统计限制。无权限的指标写 `NOT_MEASURED`。
3. 设计长运行资源预算、维护库存和背压阈值；确认 no-Adapter HIGH 在原 6 GiB 上限下无 OOM、无不可恢复阻塞，发生器无丢发；不把 memory.high 本身当故障或把没有 OOM 当性能合格。
4. 将 H2–H4 通过的候选组合固定为一个 revision/wheel/config，不纳入被拒绝试验；先运行完整模块、恢复、生命周期回归再进入 H6。发生故障时保留证据并停止，不能自动提高资源或跳过门槛。

## 11. H6：集中验收与发布门槛

各轮 fresh candidate，固定同一组合和资源，使用真实 UDS API、无 Adapter Mock，完整保留 raw、manifest、失败及退出证据。先连续通过 LOW、HOT 与故障关闭，再执行 HIGH；某轮失败保留，修复后重新冻结 revision 并按受影响范围重跑，不能用旧 revision 补齐新 revision 的三轮。

| 验收 | 最低要求 |
| --- | --- |
| LOW ×3 | 每轮 10 秒，500 Submit TPS＋500 Snapshot QPS；各 5,000 全部窗口内完成，零未发/传输/业务错误；PollEvents、source、最终 drain 与 lifecycle 符合 25 号方案。 |
| HOT ×3 | 每轮 10,000 预热＋10,000 正式单在途；正式请求全部成功、LSN 唯一，scheduled P95 ≤1.50 ms、P99 ≤3.00 ms。 |
| 故障/关闭 | 执行 H2–H4 所列真实 Linux 进程故障、持久化/恢复、安全和受控关闭；所有子进程、credit 和生命周期证书闭合。注入故障轮不得冒称正常受控退出。 |
| HIGH ×3 | 每轮同一 60 秒窗口并发 2,000 Submit/s＋2,000 Snapshot/s，各 120,000 计划请求全部成功且窗口内完成，零错误、漏发及容量丢弃。 |
| HIGH 延时 | Submit scheduled P95 ≤20 ms、P99 ≤60 ms；Snapshot scheduled P95 ≤10 ms、P99 ≤40 ms；PollEvents call→FULL progress P95 ≤10 ms、P99 ≤40 ms。 |
| 事件与证据 | 按冻结 fixture 核对源事实/唯一事件；当前模型每轮最终 240,800 事件，最终 source、projector、checkpoint、generation、lifecycle 一致且逐条匹配。窗口内事件和窗后 drain 分开报告；如果 fixture 改变，必须预先说明和冻结目标，不能沿用旧计数。 |
| 资源与资格 | 满足原资源、空间及完整采样门槛；无 OOM/kill、异常泄漏或无界积压。证据、核隔离和配置证明不足时为 `INCOMPLETE`，不能签发正式资格。 |

每轮输出请求和事件实际完成率、窗内/窗后/未发/失败、逐路径 P50/P95/P99/P100、两端分段和 source→FULL，附计划负载及吞吐分母；同步记录资源、trace 完整性、generator queue 和 clock identity。PollEvents 达标不等于源事件积压已消除。任何一个必需条件失败，整体为 `FAILED` 或 `INCOMPLETE`，不得以其他轮或其他指标抵消。

HIGH 通过也不自动证明 12 小时长运行、真实 EMT、权威恢复或交易 `READY`。这些必须按各自合同独立验收。

## 12. H7：部署、验证与回退

1. H6 通过后才可安排正式部署；先记录现有正式服务、版本、配置、进程、健康、资源与回滚点，确认该服务入口确实使用已验收拓扑；候选通过不能直接当普通 unit 已具备该拓扑。
2. 按既有 Git 流程 push、在服务器从对应分支拉取，构建并安装已验收 wheel；核对 commit、wheel hash、配置 hash 与运行进程载入路径。不得用临时文件覆盖生产或更改其他项目。
3. 按已批准的维护窗口操作，检查 UDS 生命周期、权限、连接、只读健康及受控退出；业务状态保持原有 DISARMED/READY 边界，不启动真实柜台测试。部署后对实际入口执行与风险相称的只读/Mock 回归，并记录影响与证据。
4. 若健康、配置、功能或证据失败，按预先记录的版本和配置回滚，保留数据与失败证据；恢复后核验原服务健康。已提交订单的未知结果不得自动重发；回滚不删除 Journal、投影库或凭证。

## 13. 阶段记录模板

每个阶段在实施记录或独立报告中追加：

```text
阶段 / 状态：
代码 commit、分支、远端核验 / wheel hash / 配置与工具 hash：
目标假设、变更点、默认开关及不变量：
测试环境、资源、namespace、负载、预热/窗口/drain：
模块/故障/恢复验收与未执行项：
A/B 次序、全部轮次与逐笔证据路径及 hash：
请求/事件守恒、P50/P95/P99/P100、资源/trace 缺项：
结论、失败及回退、下一阶段门槛：
```

本文建立时只完成方案编写，前述历史测量不自动构成这些阶段的通过证据。

### 2026-10-27：H0 trace 完整性校验子包

状态：`IMPLEMENTED_NOT_ACCEPTED`。handoff 分析器对旋转 trace 核验 closed manifest、逐段 hash/sequence、代和文件集合；新增严格选项 `--require-hash-chain`，拒绝没有完整链的 writer。未选择严格选项的报告明确为 `NOT_ASSERTED`，不能作为全部 writer 的哈希完整性证明。`15fddbb` 的四组相关测试 Windows 44 passed、Linux 44 passed；Linux 使用 Git 独立检出，非生产部署。完整阶段与性能验收尚未通过。

既有 2026-10-27 broker 诊断为单文件无 hash-chain，不能在缺证的情况下补称完整链通过；H0 的发生器自检、全路径配对和集中验收仍未完成。H1–H7 保持 `DESIGNED`，不得据此部署。

后续子包将默认关闭的 broker 诊断 writer 切换为有界旋转 hash-chain，原 64 MiB 配额与信用/提交路径不变；旧数据不补造 hash。Windows 定向测试 23 passed、2 skipped，Linux、完整配对和 trace 开销仍待实际验收。该变更不会自动开启诊断或证明 H0/H1 完成。

`d797bd1` broker 子包 Linux 定向测试 25 passed；独立受控发生器复测 10 秒、2,000＋2,000 每类各 20,000 全部窗内、无漏发，计划→调用 P99 分别 1.543/1.552 ms，工具 revision 正确，fixture 通过。证据 `/home/xuanling/tradecore-h0-generator-d797bd1-20261027-r2`。第一次 fixture 虽无漏发、发生器 P99 约 1.7 ms，但请求总时延超过工具门槛、Git revision 缺失，整轮失败，证据 `/home/xuanling/tradecore-h0-generator-d797bd1-20261027`；不得以复测覆盖该失败。本项只证明发生器，不证明 TradeCore 性能。H0 完整逐请求配对仍未验收。

### 2026-10-27：H1 维护分段证据子包

状态：`IMPLEMENTED_NOT_ACCEPTED`。新增仅在 `TRADECORE_DIAGNOSTIC_SEGMENT_TRACE_DIR` 显式配置时启动的维护进程有界旋转 trace，记录 prepare/seal 操作及截断、分配、预触页、flush、fsync、校验和 hash 的 BEGIN/END/FAILED；不记录正文、路径或异常文本。Windows 定向 45 passed、1 skipped。父进程 manifest 发布、切换与完整组合关联、真实 Linux 故障、采样开销和因果 A/B 仍待验收；不能据该子包证明 H1 或 H2 通过，也不能部署到正式服务。

`0d04574` 的维护、Journal 和 trace 相关回归 Linux 54 passed。新增维护 trace 离线分析子包校验 hash-chain、clock、进程/Journal 身份及 BEGIN/END 配对；Windows 13 passed、1 skipped，Linux 验证及和请求/资源的联合归因仍待完成。此结果不改变 H1 整体状态。

`48d32d8` 的相同 Linux 回归 54 passed。使用从 Git 拉取的独立 source 与 transient unit（非正式服务）执行 10 秒 2,000＋2,000 no-Adapter 联合诊断：每类 20,000 全部窗内，最终 40,800 事件与 fresh Mock source/lifecycle 闭合。frontend/broker/owner 全部旋转 writer hash-chain 通过，20,400 条全量、20,000 条窗内 Submit 逐笔配对。20,000 笔 frontend offer→owner receive P95/P99 为 8.438/37.929 ms，send 完成→owner receive 下界 P95/P99 为 8.029/37.747 ms；该诊断有 trace 扰动，仅一轮，`formal_qualification=false`。

维护 trace 闭合 58 条记录、5 次操作、无 drop/失败；测量窗内一次 seal 持续约 1,087.604 ms，其中校验 809.612 ms、flush 140.767 ms、digest 128.803 ms；一次 prepare 持续 169.033 ms。按请求 planned 时刻与维护操作重叠分类，Submit 在 seal 窗口的 2,200 条 scheduled P95/P99 为 48.080/56.021 ms，非维护窗口 17,460 条为 9.330/13.084 ms；Snapshot 分别 4.694/7.735 ms 与 4.640/7.703 ms。这是样本分组相关，不是受控因果对照，时间分位不可相减；该轮未同步采集完整资源样本，不得据此宣称 H1、H2 或 HIGH 验收通过。诊断目录 `/home/xuanling/tradecore-h1-trace-48d32d8`，candidate 根 `/var/lib/tradecore-performance-candidates/h1-git-48d32d8/r`；正式服务未重启，候选已受控停止。

同一 `48d32d8`、固定资源和独立 fresh candidate 的 HOT 诊断开销交错 A1/B1/B2/A2/A3/B3：每轮 10,000 预热＋10,000 正式单在途，均取得 10,000 accepted。A 为不启 frontend/broker/segment 诊断，B 启用三项；P95/P99 毫秒分别为 A1 `1.374/2.613`、B1 `1.221/2.497`、B2 `1.193/2.558`、A2 `1.233/2.460`、A3 `1.157/2.566`、B3 `1.104/3.507`。B3 超过原 HOT P99≤3 ms，**诊断开销绝对门槛未通过，H1 不通过**；各轮不得挑选或平均后抵消失败。B3 分组中非维护期间 8,193 样本 P99 3.488 ms，不能把失败单独归因于 segment seal；未采集足以解释该轮变化的连续资源/逐笔热态配对，根因未证实。全部候选已停止，正式服务 active。各轮证据目录 `/home/xuanling/tradecore-h1-hot-48d32d8-{a1,b1,b2,a2,a3,b3}`；首次准备时 sudo 缓存限制导致六个配置步骤失败，未启动进程，修正认证后才执行以上各轮。不得使用此对照升级正式性能资格。

### H3 补充：HOT 慢样本分段定位

状态：`IMPLEMENTED_NOT_ACCEPTED`。对 B3 的 10,000 正式样本与 frontend/broker/owner 关联均各找到 10,000 条；其中按时间顺序筛选，133 条端到端≥3 ms 的样本中，owner receive→finish P50 2.714 ms，send→owner receive 上界 P50 0.613 ms；正常低于 1.5 ms 的 9,706 条中分别为 0.240 ms、0.020 ms。此为样本分组关联，不证明 trace 开销或特定代码是原因；1 条边界异常未纳入分段统计，不能按完整正式配对验收。

为继续区分 owner 业务后阶段，新增默认关闭的 `TRADECORE_DIAGNOSTIC_OWNER_DETAIL=1`，记录 context exit、Journal read bound、response correlation 和 COMPLETED send 的前后时点；仅诊断字段，不改变提交、顺序或响应语义。Windows 相关测试 44 passed、7 skipped、6 subtests passed；完整 Linux 故障及三组对照尚待验收。H3 整体未完成。

`92c9ce9` Linux 真实 owner 子进程测试开关关闭/开启各一次均通过（2 passed），保留证据 `/home/xuanling/tradecore-h3-owner-evidence-92c9ce9`；首次试跑因缺少私有 evidence root 在启动前失败，两项失败不可记为通过。另在独立 transient candidate 进行 10,000 预热＋10,000 单在途诊断，10,000 accepted，HOT P95/P99 `1.266/2.489 ms`，10,000 样本均关联到 owner 记录。端到端≥3 ms 仅 21 条：其中 admission P95 10.785 ms、COMPLETED emit P95 2.598 ms，context exit 和 response check P95 分别 0.003/0.014 ms；小样本分布不能证明唯一瓶颈，也不能推断 CPU、GIL 或 I/O 因果。该次为单轮、含详细 trace，不能覆盖上一组 B3 失败或升级 H3/HIGH。证据 `/home/xuanling/tradecore-h3-hot-detail-92c9ce9`，候选已停止，正式服务保持 active。

追加子包 `815a71e` 在同一默认关闭开关下，为 actor 和 COMPLETED send 增加线程 CPU 前后时间，用于区分本线程运行与墙钟等待；CPU 差额仍不能直接证明 GIL、块 I/O 或调度根因。Windows 相关测试 37 passed、7 skipped；Linux 真进程开关关闭/开启两项通过，证据 `/home/xuanling/tradecore-h3-owner-evidence-815a71e`；采样开销及组合对照仍待验收。

`815a71e` 的第一轮 60 秒 HIGH 详细诊断中，Submit/Snapshot 各 120,000 窗内成功，Submit scheduled P95/P99 `32.932/90.767 ms`、Snapshot `5.648/11.166 ms`、PollEvents call→FULL progress `11.616/21.807 ms`；Submit 与 PollEvents 未满足原门槛，窗内 185,110 / 最终 240,800 事件。frontend/broker/owner 旋转 trace 全部 hash 验证且窗内 120,000 条 Submit 一对一；frontend→owner P95/P99 `30.584/62.231 ms`。8,544 条 scheduled≥20 ms 的样本中，owner COMPLETED emit 墙钟 P99 3.655 ms、线程 CPU P99 0.023 ms；此项是各自分位、不能相减当单笔分位。该轮使用新的临时 unit 名称，资源采样器按白名单拒绝，资源证据为 `NOT_MEASURED`；候选已停止、正式服务 active。证据 `/home/xuanling/tradecore-h3-high-detail-815a71e`，不是正式资格。

第二轮改用已允许的 `tradecore-performance-candidate.service`，不改变内核或宿主配置：Submit/Snapshot 各 120,000 窗内成功，Submit P95/P99 `24.805/75.104 ms`、Snapshot `5.399/8.937 ms`、PollEvents `11.772/18.776 ms`；Submit 和 PollEvents 仍超门槛，窗内 177,507 / 最终 240,800 事件。资源采样 105/105，但 `cgroup.io.stat`、逐任务 block-I/O 计时各缺 105 次，资源证据仍 `complete=false`，不能把缺失记为 0。候选第 43 秒 owner emit 墙钟 P99 3.929 ms、线程 CPU P99 0.015 ms，同期 memory.high 增量 36；第 42 秒分别 3.277/0.021 ms、high 增量 229。这些是 1 秒区间与分位相关，不是单笔 CPU 差或因果证明。证据 `/home/xuanling/tradecore-h3-high-detail-815a71e-r2`，已停止候选、正式服务 active。两轮均为诊断并失败，不能以最终 drain 或两类成功数量代替 HIGH、H1/H3 及生产部署资格。

第二轮经离线 source/checkpoint/lifecycle 逐事件 join，240,800 个事件匹配，producer closure 为 `MATCHED_FRESH_MOCK_SOURCE`；全部事件 source→FULL 上界 P95 约 20.183 秒，该口径包含窗口外 seed 和 drain，不等于稳定窗口的单次 PollEvents 延迟。闭合不改变该轮失败及资源采样缺失的结论。

后续诊断子包继续将 `COMPLETED` 编码与 socket `sendall` 的前后墙钟及后者线程 CPU 拆开，开关仍默认关闭；有部分写/超时的请求不会获得伪造的发送完成时点，原 UNKNOWN/fence 行为不变。Windows 协议及 owner 相关回归 111 passed、8 skipped；完整 trace 开销与组合性能验证尚未完成。仅凭墙钟与线程 CPU 差仍不能把等待确认为 GIL、内核 I/O 或调度。

`5d96b09` Linux 协议、故障及真实 owner 子进程回归 119 passed；独立 10 秒 HIGH 诊断 Submit/Snapshot 各 20,000 窗内成功，Submit scheduled P95/P99 `12.378/46.236 ms`，Snapshot `4.624/7.823 ms`，PollEvents `7.479/10.052 ms`，单次诊断不代表完整 HIGH。对 440 条 COMPLETED emit≥1 ms 样本逐条拆分，emit 墙钟 P50/P95 `1.945/3.730 ms`，编码完成→开始 send P50/P95 `1.925/3.719 ms`，实际 send 墙钟 P50/P95 `0.004/0.008 ms`；此中间段包含 deadline、gettimeout/settimeout 和线程调度，不能直接归责于 socket 发送。证据 `/home/xuanling/tradecore-h3-packet-5d96b09`，候选停止、正式服务 active。为继续定位，后续默认关闭诊断进一步在 gettimeout/settimeout 两侧记录墙钟及后者线程 CPU；Windows 相关测试 111 passed、8 skipped，Linux 与完整对照待验收。

`c0c12c4` Linux 协议及真实 owner 回归 119 passed。独立 10 秒 HIGH 诊断每类 20,000 全部窗内，Submit P95/P99 `17.859/49.807 ms`、Snapshot `4.649/8.012 ms`、PollEvents `6.812/8.593 ms`；非正式验收。432 条 emit≥1 ms 样本中 `settimeout` 墙钟 P50/P95 为 `1.802/3.493 ms`、线程 CPU 为 `0.003/0.006 ms`；gettimeout、实际 send 的 P50/P95 分别接近 `0/0 ms`、`0.004/0.008 ms`。这是分组诊断，不证明墙钟等待具体由 GIL 或 OS 调度造成。证据 `/home/xuanling/tradecore-h3-timeout-c0c12c4`，候选已停止、正式服务 active。

基于此新增默认关闭的 Linux `TRADECORE_DIAGNOSTIC_OWNER_SEND_NONBLOCKING=1` 试验：独占的无 timeout 原 socket 上使用 `MSG_DONTWAIT`，仅内核返回 EAGAIN 才按原绝对 deadline 等待 writable，逐字节发送同一有界 packet；不改变 socket 模式、原始顺序、单 writer、credit、UNKNOWN/fence 或重试合同。该候选不是已采纳方案，Linux 实际部分发送/超时、owner 进程故障、三组 A/B、LOW/HOT/HIGH 和正式组合均待验证；Windows 本地 111 passed、12 skipped。超时、EOF、部分发送不可作为安全重试新单。

`7da0c89` Linux 协议、真实部分发送/超时与 owner 故障相关套件在候选默认关闭及显式开启下各 123 passed；证据 `/home/xuanling/tradecore-h3-owner-evidence-7da0c89`、`/home/xuanling/tradecore-h3-nonblocking-fault-7da0c89`。同 revision、固定资源、no-Adapter 的 10 秒 2K+2K 六轮交错 A1/B1/B2/A2/A3/B3，各轮 Submit/Snapshot 各 20,000 全部窗内，最终各 40,800 事件至 drain，尚未逐事件 source-join。Submit scheduled P95/P99 分别 A1 `11.010/45.810`、B1 `10.961/40.567`、B2 `11.895/42.959`、A2 `10.519/34.439`、A3 `11.411/49.316`、B3 `11.043/39.322 ms`；Snapshot P95/P99 均 `4.452–4.608/7.377–7.918 ms`，PollEvents P95/P99 均 `6.255–6.869/8.073–9.093 ms`。第二组候选较基线退化，六轮不能证明稳定收益；不能挑选好轮、不能将短轮或模块故障测试当作完整组合/性能验收。证据 `/home/xuanling/tradecore-owner-send-ab-7da0c89-{a1,b1,b2,a2,a3,b3}`，候选保持默认关闭，H3、H6、H7 未通过。

第二轮 `815a71e` 60 秒诊断按请求 planned 时刻与维护时段分组：prepare 窗口 2,980 条 Submit P95/P99 `105.235/144.657 ms`、Snapshot `32.835/69.516 ms`；非维护窗口各 105,100 条分别 `15.409/66.507 ms`、`5.230/8.531 ms`。分组存在时间、负载和资源混杂，只能支持可复现关联，不能推断因果；维持 `H1 FAILED/INCOMPLETE`。为开展对照，新增默认关闭的 `TRADECORE_DIAGNOSTIC_PACED_PREFAULT=1`，仅由维护子进程在预触页每 256 页暂停 1 ms；ready 前原有预触页、flush、fsync、manifest、验证均不跳过。Windows Journal 定向测试 46 passed、1 skipped，另含真实子进程准备的测试 15 passed、1 skipped；Linux 故障、三组 A/B、库存不足、低压及整体性能均待验收。该诊断试验不表示 H2 已满足前提或可部署。

`9ce311e` 显式开启节奏候选的 Linux Journal/段恢复/owner 相关套件 75 passed，证据 `/home/xuanling/tradecore-h2-fault-9ce311e`；这不是全部 H2 故障矩阵。相同 revision、固定资源与 trace 的 60 秒 2K+2K 交错 A1/B1/B2/A2/A3/B3 中，A 为原预触页，B 为节奏候选；各轮两类各 120,000 全部窗内、零业务/传输错误。每轮最终 240,800 事件均与 fresh Mock source/lifecycle 逐条闭合，窗内事件分别 181,786/182,218/178,529/182,556/180,228/182,016。各轮 P95/P99，单位 ms：

| 轮次 | Submit | Snapshot | PollEvents call→FULL progress |
| --- | --- | --- | --- |
| A1 | 39.846 / 116.195 | 6.128 / 21.791 | 13.091 / 26.968 |
| B1 | 21.021 / 64.225 | 4.846 / 8.172 | 10.813 / 18.252 |
| B2 | 19.038 / 59.408 | 4.825 / 8.106 | 10.035 / 15.950 |
| A2 | 35.095 / 72.483 | 5.231 / 8.520 | 11.026 / 17.580 |
| A3 | 26.788 / 73.269 | 5.083 / 8.479 | 10.838 / 17.973 |
| B3 | 16.384 / 44.040 | 4.628 / 7.684 | 10.404 / 16.269 |

候选三轮 Submit P95/P99 均较对应基线低，但 B1 仍超 Submit 原门槛，B1/B2/B3 的 PollEvents P95 均超过 10 ms；B3 子运行器退出码为 0 只代表该子工具检查通过，**不能替代 PollEvents 或完整 HIGH 门槛**。预触页各候选约 357–385 ms、基线约 78–111 ms，必须继续验证备用库存和故障边界。每轮资源采样均缺 `cgroup.io.stat` 及逐任务 block-I/O 计时，`complete=false`；未完成全 H2 故障矩阵和三轮 LOW/HOT。当前候选继续默认关闭、`IMPLEMENTED_NOT_ACCEPTED`，不得部署，H1/H2/H6/H7 均未通过。六轮证据 `/home/xuanling/tradecore-prefault-ab-9ce311e-{a1,b1,b2,a2,a3,b3}`；正式服务保持 active。

按候选三轮 PollEvents 逐条拆分，call→FULL P95 为 10.812/10.030/10.400 ms，RPC P95 3.020/3.107/3.246 ms，response→FULL P95 8.810/8.333/8.474 ms；各分位不能相加。基于此增加默认关闭的更平缓固定试验 `TRADECORE_DIAGNOSTIC_PACED_PREFAULT=64`（每 64 页暂停 1 ms）；旧实验值 `1` 仍表示每 256 页，非法值 fail-closed，默认值 `0` 不暂停。Windows 段/恢复相关测试 50 passed、1 skipped。Linux、库存门禁、三组 A/B 和最终验收仍待执行，不得提前启用。

`767b35b` 的 64 页候选 Linux Journal/恢复/owner 相关回归 78 passed；证据 `/home/xuanling/tradecore-h2-fault-767b35b`。同 revision、固定资源及维护 trace 的 60 秒 2K+2K A1/B1/B2/A2/A3/B3，对照 A 为每 256 页、B 为每 64 页。P95/P99 毫秒如下：

| 轮次 | Submit | Snapshot | PollEvents call→FULL progress | Submit 窗内 |
| --- | --- | --- | --- | ---: |
| A1 | 25.166 / 71.107 | 5.091 / 8.413 | 11.182 / 19.939 | 120,000 |
| B1 | 26.673 / 57.180 | 4.764 / 7.836 | 11.321 / 18.235 | 119,980 |
| B2 | 27.525 / 74.580 | 5.198 / 8.610 | 12.345 / 20.169 | 120,000 |
| A2 | 39.092 / 102.367 | 5.816 / 15.852 | 12.902 / 22.626 | 120,000 |
| A3 | 23.937 / 58.425 | 4.960 / 8.040 | 11.575 / 19.907 | 120,000 |
| B3 | 29.639 / 64.913 | 4.944 / 8.139 | 12.173 / 18.022 | 120,000 |

每轮最终各 120,000 Submit、120,000 Snapshot 成功，final drain 各达 240,800 事件，经 source/checkpoint/lifecycle 逐条 join 六轮均为 `MATCHED_FRESH_MOCK_SOURCE`；但 B1 的 20 笔 Submit 窗后完成，六轮 PollEvents P95 全超 10 ms，资源采样每轮缺 `cgroup.io.stat` 和逐任务 block-I/O 计时。闭合不抵消上述失败。64 页试验无稳定收益且产生窗后完成，标记 `NOT_ADOPTED_WITH_EVIDENCE`，保持默认关闭；256 页也仍是未验收候选。证据 `/home/xuanling/tradecore-prefault64-ab-767b35b-{a1,b1,b2,a2,a3,b3}`。全部候选已停止、正式服务 active。

服务器只读设备核对：候选 Journal 位于根盘 `/dev/nvme0n1p3`，消费者测试 checkpoint 同样位于该根盘；另一个 NVMe 已承载 `/data` 上其他服务。仅凭共同设备不能证明 I/O 因果，也不授权占用或改动其他服务存储。六轮结束根盘剩余约 466 GiB，已保留所有本轮证据，不自动删除。

### H4 补充：事件分片并发诊断

`767b35b`、256 页节奏候选、相同固定资源下分别试验单分片与双分片事件消费者，各一轮 60 秒、2,000 Submit/s＋2,000 Snapshot/s；不是三组交错 A/B，也未证明核独占。两轮两类请求均各有 120,000 窗内完成，最终 drain 均到达 240,800 条事件，source/checkpoint/lifecycle 离线 join 均为 `MATCHED_FRESH_MOCK_SOURCE`；其中双分片运行器退出非零，不能以成功计数覆盖失败。单分片 Submit P95/P99 `18.711/45.318 ms`、Snapshot `4.551/7.795 ms`、PollEvents call→FULL `10.781/17.056 ms`；双分片分别 `37.945/88.408 ms`、`5.132/9.126 ms`、`22.315/47.874 ms`。这是单次诊断、存在运行环境波动，不能得出严格因果，但双分片本轮同时劣化两条路径，且 PollEvents 超原门槛，不得采纳或宣称 H4 通过。证据 `/home/xuanling/tradecore-event-partition-767b35b-p{1,2}`；资源样本各缺 I/O 及 block-I/O 计时。

### H1/H5 补充：授权后的资源采样

用户授权临时开启全机 `kernel.task_delayacct` 和 TradeCore 父 cgroup I/O 控制器，并要求结束后恢复。原值分别为 `0`、未开启；两次诊断均预先设置独立的 300 秒兜底恢复计时器，结束时先停止本项目候选、恢复原值并确认，再停用计时器；正式 Consumer UDS 全程保持 active。第一次 `/home/xuanling/tradecore-resource-authorized-767b35b-r1` 的负载生成器因交易日不是 ISO 格式而在发压前退出，即使采样完整也不能作为负载测试或验收证据。

修正日期后 fresh candidate 的第二轮 `/home/xuanling/tradecore-resource-authorized-767b35b-r2` 取得 110/110 完整资源样本、missing 空，60 秒窗内 Submit 和 Snapshot 各 120,000 完成；Submit scheduled P50/P95/P99/P100 `5.403/29.409/65.405/253.886 ms`，Snapshot `2.779/4.960/8.221/138.543 ms`，PollEvents call→FULL `4.485/12.329/20.120/278.659 ms`。Submit 与 PollEvents 不满足原 HIGH 门槛，运行器退出非零，整体失败；最终 240,800 事件 source/checkpoint/lifecycle 匹配不抵消失败，source→FULL 上界 P95 约 17.968 秒包含种子和窗口外 drain，不等于单次 PollEvents 延迟。

同一测量窗的 60 个一秒资源样本中，候选 cgroup 写入字节从 827,895,808 增至 8,556,498,944；`memory.events high` 增量 1,093，`max/oom/oom_kill` 增量均为 0，CPU throttled 增量为 0。单秒写入增量最大约 608.63 MiB；任务 block-I/O 计数最强的一个一秒区间内，跨存活线程计数增量相加为 93 ticks（内核 HZ=100，线程等待可重叠，不是墙钟时间），同秒 memory.high 增量 230。I/O 设备统计与任务等待属于采样关联，不能据此认定单笔请求因果，也不能将未采集的设备物理延时标为零。全部采样仅代表本隔离候选，不证明 CPU/IRQ 独占或正式生产性能。

### H1/H4 补充：逐笔时段拆解与封存缓存试验

对上述 R2 的四个 shard 客户端 raw 文件校验 SHA-256 后按逐笔时间计算：120,000 条 Submit scheduled P95/P99 `29.394/65.373 ms`，其中 planned→call 为 `1.152/1.239 ms`，call→connect 为 `0.570/0.642 ms`，send→response 为 `28.183/64.152 ms`；分位数不可相减，这只表明主要延迟落在发送后的等待区间，不能直接将其全部归因于服务器。测量窗内 6,992 条 PollEvents 按 progress 逐条拆解 call→FULL P95/P99 `12.328/20.104 ms`，RPC `3.284/5.825 ms`，response→FULL `10.038/13.088 ms`，其中 FULL commit `9.404/12.024 ms`。Progress 的逐条记录用于诊断，不能以此次分析替代完整 trace 验收。Submit 第 34 秒与 PollEvents 第 34、42、50 秒存在共同波动；一秒采样和未配对的分位不能证明设备 I/O 导致所有长尾。

新增默认关闭的 `TRADECORE_DIAGNOSTIC_SEALED_CACHE_EVICT=1` 候选：仅在已有封存校验、flush、fsync、digest、catalog 发布及原映射关闭后，维护线程对未归档的封存文件调用 Linux `POSIX_FADV_DONTNEED`。目录、文件与事实均不删除；建议并不保证内核实际回收页面。归档锁防止与已验证归档的迁移竞态，任何异常使维护 fail-closed；无效开关或不支持平台拒绝启动。此候选可能影响后续 replay 的缓存命中，仅用于隔离 A/B；实际 Linux 恢复/故障、缓存和 I/O 影响、三组 A/B、LOW/HOT/HIGH 均待验证，未获准启用到正式服务。

`51f145d` Windows Journal/归档/段子进程定向回归 54 passed、5 skipped；broker/owner/重放/消费者 checkpoint 定向回归 94 passed、6 skipped。Linux Journal、归档、段子进程和 owner 定向回归在默认关闭及环境显式开启时各 67 passed；包含注入 advice 失败后的 fail-closed 与重开验数，但**不等同完整 Linux 断点 SIGKILL/恢复矩阵**。

相同 revision 与 256 页预触页节奏下，以 A=关闭、B=开启缓存建议，执行 A1/B1/B2/A2/A3/B3 交错 60 秒、2K Submit/s＋2K Snapshot/s。每轮两类请求各 120,000 在窗口内完成，最终 drain 各 240,800 条事件；六轮 fresh Mock source/checkpoint/lifecycle 均逐条 `MATCHED_FRESH_MOCK_SOURCE`。P95/P99（ms）：

| 轮次 | Submit | Snapshot | PollEvents call→FULL |
| --- | --- | --- | --- |
| A1 | 51.905 / 173.539 | 7.119 / 34.275 | 12.870 / 25.100 |
| B1 | 27.935 / 56.361 | 4.825 / 7.930 | 10.871 / 18.072 |
| B2 | 42.861 / 74.711 | 5.308 / 8.700 | 12.640 / 21.332 |
| A2 | 36.635 / 94.044 | 5.788 / 15.589 | 12.411 / 19.431 |
| A3 | 24.953 / 64.291 | 4.927 / 8.135 | 12.272 / 21.938 |
| B3 | 37.421 / 72.810 | 5.100 / 8.430 | 12.607 / 21.070 |

B1 好于 A1，但 B2 的 Submit P95 和 PollEvents P95 劣于 A2，B3 的 Submit P95/P99 与 PollEvents P95 劣于 A3；所有开启组均未达到原 Submit/PollEvents HIGH 标准。六轮资源采样各 110 个，但 cgroup I/O 与 per-task block-I/O 计时未开启，均为 `complete=false`；不能将此次实验解释成证实缓存回收改善 I/O。候选标为 `NOT_ADOPTED_WITH_EVIDENCE`，继续默认关闭，不能部署；H2/H4/H6/H7 不因此通过。证据 `/home/xuanling/tradecore-cache-advice-51f145d-{a1,b1,b2,a2,a3,b3}`，不删除失败轮。

六轮后对服务器的只读线程快照中，其他服务线程的最近运行 CPU 与候选 AllowedCPUs 集合有重叠；该快照发生在测试之后，不证明测试窗内实际竞争，但足以否定仅由本项目亲和性推断物理核独占。H5 资源隔离资格仍未建立，不能以六轮诊断数据补签正式验收。不得为制造独占而擅自移动、重启或限制其他服务。

后续只读核对显示 `tradecore.slice/cpuset.cpus.partition=root`，`cpuset.cpus.exclusive.effective=64-79,160-175`，而 `/sys/devices/system/cpu/isolated` 为空。前者说明该 cgroup 已配置 cpuset 分区及其有效 exclusive CPU 集合，不能忽略；它本身也不能证明测试窗内所有其他任务和 IRQ 都未竞争这些核。应结合同窗实际线程调度及 IRQ 证据核验，不能仅根据事后快照认定配置失效，也不能仅根据 `AllowedCPUs` 或 exclusive 文件证明完全无干扰。

### H3 补充：owner 线程身份归因

对授权资源轮 `/home/xuanling/tradecore-resource-authorized-767b35b-r2` 的同一单调时钟区间按完整原始 Submit 和资源样本复算，最高长尾的一秒约有 2,000 条 Submit，其 scheduled P95/P99 `154.12/196.16 ms`；同秒候选 cgroup 写入增量约 302.9 MiB，owner 进程已持续存在的线程 runqueue 等待增量合计约 992 ms、block-I/O 等待增量 15 ticks，`memory.high` 增量为 0。另一高峰区间 cgroup 写入约 608.6 MiB，其中段维护 CPU 77 上某进程记账约 256 MiB、owner 线程 runqueue 等待增量合计约 1,023 ms。不同线程等待可重叠，累加不是单笔或墙钟延迟；进程 write_bytes 和 cgroup I/O 是不同记账口径，不能混加。这些同时段样本提示维护、调度和 I/O 都需排查，不证明其任何一个是唯一根因。

此前资源样本只有 Linux TID 和 start ticks，无法可靠区分单提交 actor 与 Mock、索引、trace 等后台 Python 线程；之前把整个后台迁到 frontend 物理核或其 SMT 兄弟的试验已有退化证据，不盲目重复。新增默认关闭的 `TRADECORE_DIAGNOSTIC_OWNER_THREAD_IDENTITIES=1` 启动快照：记录 owner PID、Python 已知角色白名单、Linux native TID、线程 start ticks、CPU 亲和性及单调时钟身份；未知名称只写 `OTHER`，不记录账户、订单或任意线程名。仅进程启动时采集一次，不在热路径写文件；之后新建或已退出线程明确不在覆盖内。按 PID/TID/start ticks 与同钟资源采样连接才能判断身份，不能凭 TID 数值单独连接。Linux 实际 owner 组合和诊断扰动评估待验收，不代表 H3 已完成。

`5faea7e` 的独立 no-Adapter 短轮采集到 14 个启动线程，身份字段均完整，其中一个角色为 OTHER；同钟资源采样中有 9 个完整一秒间隔。按 PID/TID/start ticks 配对，`oms-inherited-write` 累计线程 CPU 约 4.600 秒、调度等待约 1.701 秒，`consumer-mock-lifecycle` 分别约 1.782 秒、1.544 秒；`source-commit-evidence` CPU 约 0.226 秒、等待约 1.616 秒，两条 `consumer-trace-writer` 合计 CPU 约 0.185 秒。全部线程当时绑定 CPU 64；线程等待可重叠，不能相加视为单笔延迟，启动快照也不覆盖后续新建线程。该 10 秒轮 Submit/Snapshot 各 20,000 窗内完成，Submit scheduled P95/P99 `11.174/43.352 ms`，PollEvents call→FULL `6.644/8.684 ms`；最终 40,800 条经 source/checkpoint/lifecycle 逐条闭合。资源样本缺 I/O/逐任务 block-I/O，短轮及快照开销尚未验收；证据 `/home/xuanling/tradecore-thread-map-5faea7e-r1`。不据此宣称 HIGH 或该组合已通过。

据此新增默认关闭的单因素候选 `TRADECORE_DIAGNOSTIC_EVIDENCE_CPUS`：仅在隔离候选启动时把 `source-commit-evidence` 和 `consumer-trace-writer` 迁至显式 1–2 个现有 cgroup CPU；WRITE actor、Mock 及其他线程不迁移。要求核不与 WRITE 的现有集合重叠，读取内核实际亲和性，CPU 被裁剪、线程缺失或身份变化时拒绝启动；迁移中途失败尽力恢复已经修改的存活线程。候选试验选 `171,172`，这两个是 frontend 使用的物理核 75/76 的 SMT 兄弟，**不声称独占**，可能损害 frontend，需要配对 A/B 和 LOW/HOT/HIGH 检查，不能先行采用。源码、Linux 真进程、完整故障与组合门禁均需验证。

`fe1dd11` 本地定向回归 31 passed/7 skipped，Linux 定向 32 passed/6 skipped；单组 fresh no-Adapter 10 秒 A/B 真实启动快照确认 A 的三条证据线程均在 CPU 64，B 迁至 `171,172`，两侧 WRITE actor 与 Mock 均在 CPU 64。每侧 Submit/Snapshot 各 20,000 窗内完成，最终 40,800 事件与 source/checkpoint/lifecycle 逐条匹配；Submit scheduled P95/P99：A `11.059/47.415 ms`，B `35.029/60.817 ms`；Snapshot `4.616/7.827` 对 `5.165/8.032 ms`；PollEvents call→FULL `6.713/8.552` 对 `6.889/8.692 ms`。B 超原 Submit 门槛，按审慎策略停止此方案，标为 `NOT_ADOPTED_WITH_EVIDENCE`，默认关闭，不进入正式部署。

按 startup PID/TID/start ticks 与同钟采样连接的 9 个完整一秒区间，A→B 的 WRITE actor CPU `4.533→5.287 秒`、runqueue 等待 `1.723→1.870 秒`；两条 trace writer 的 CPU `0.186→0.625 秒`，source evidence CPU `0.223→0.564 秒`，相应 runqueue 等待明显下降。不同线程时间可重叠；这是单组和诊断扰动下的观察，不能以分位相减或声明迁核导致唯一根因。两轮采样均缺 I/O 和 block-I/O 计数；未执行三组、LOW/HOT 或完整故障矩阵，不能据此签发性能资格。证据 `/home/xuanling/tradecore-evidence-affinity-fe1dd11-{a1,b1}`；正式服务未重启。

针对既有 owner CPU profile 中重复 `require_text` 的空白检查，离线在服务器 CPython 3.10.21 核验所有 Unicode 单字符的 `not s.strip()` 与 `s.isspace()` 结果一致；但为了保留 `str` 子类可能覆盖 `strip()` 的原有语义，完整候选需增加 `type(s) is str` 分支。同核 6 组交错微测、每轮 100 万次常见非空字段检查，原检查约 `0.1064–0.1111 s`，含类型分支的候选约 `0.1143–0.1193 s`。这是局部微测而不是端到端证书，但足以否定直接实施此分支作为可证明优化；未修改生产校验逻辑，也没有跳过动态字段验证。

`fe1dd11` 上保持 256 页预触页节奏，对已做语义和故障模块回归、但未曾通过 HIGH 的固定事实键快速验证再做一组短组合对照。无该快速分支 K1 与启用分支 K2 分别为 Submit scheduled P95/P99 `10.478/37.257` 与 `11.239/43.418 ms`，Snapshot `4.559/7.688` 与 `4.612/7.905 ms`，PollEvents call→FULL `6.705/9.052` 与 `6.705/8.462 ms`；各轮两类均 20,000 窗内，最终各 40,800 事件与 fresh Mock source/checkpoint/lifecycle 逐条闭合。短轮 Submit 未显示正向收益，亦不能单凭一组排除长窗口效果；两项候选仍默认关闭，不作为正式组合。资源采样各缺 cgroup I/O 和逐任务 block-I/O；证据 `/home/xuanling/tradecore-prefault-fact-combo-fe1dd11-{k1,k2}`。

同 revision 的 60 秒交错 CA1/CB1/CB2/CA2/CA3/CB3 中，CA 只启用 256 页预触页节奏，CB 叠加固定事实键快速验证；各轮两类各 120,000 窗内成功，最终 240,800 事件均与 fresh Mock source/checkpoint/lifecycle 逐条闭合。各轮 P95/P99 毫秒为：

| 轮次 | Submit | Snapshot | PollEvents call→FULL |
| --- | --- | --- | --- |
| CA1 | 36.766 / 90.243 | 5.583 / 14.557 | 12.976 / 22.266 |
| CB1 | 23.200 / 63.537 | 4.977 / 8.147 | 10.691 / 17.990 |
| CB2 | 18.022 / 51.020 | 4.678 / 7.852 | 10.912 / 15.704 |
| CA2 | 37.880 / 131.138 | 5.984 / 18.383 | 13.091 / 24.297 |
| CA3 | 19.694 / 54.100 | 4.817 / 7.991 | 11.665 / 19.137 |
| CB3 | 37.224 / 79.036 | 5.333 / 10.756 | 12.214 / 22.151 |

前两组 CB 优于相邻 CA，但第三组 CB 的三条路径均退化；三个 CB 的 PollEvents P95 均超过原 10 ms 门槛，CB1/CB3 Submit 也超过门槛，不能称为稳定有效组合。六轮资源采样各缺 cgroup I/O 及逐任务 block-I/O，`complete=false`；短工具退出码 0 不代替全部性能及资源门禁。保持两项诊断开关默认关闭，证据 `/home/xuanling/tradecore-combo60-fe1dd11-{ca1,cb1,cb2,ca2,ca3,cb3}`。

基于同钟线程证据，新增默认关闭的 `TRADECORE_DIAGNOSTIC_COMMIT_BATCH_DRAIN=1`：仅 source commit 证据后台线程一次取出最多 64 条**已经入队**的行，不等批；producer 仍使用原容量和非阻塞 `put_nowait`，证据顺序、字节、hash 链、限额、持久化和失败可见性均不变。默认关闭时仍运行原单行 worker，故不把候选强加到既有路径。单元对比覆盖原/候选逐字节及 hash 一致、跨段、完整账本、队列满、配额及 I/O 故障；Linux、真进程与组合性能仍待验收。证据写入并非 Journal 权威，任何监控失败仍不能把已提交事实伪装成未提交或自动重试。

`f2837c1` Windows 定向 23 passed，Linux 关联测试 71 passed；首次 Linux 测试因 `PYTHONPATH` 缺少性能工具目录在收集阶段失败，补齐路径后重跑，不计原失败为通过。同 revision、256 页预触节奏的单组 10 秒 no-Adapter A/B，A 默认单行、B 立即批量取出；两侧 Submit/Snapshot 各 20,000 窗内完成，各 40,800 事件与 fresh Mock source/checkpoint/lifecycle 闭合。Submit scheduled P95/P99 为 A `10.625/38.568 ms`、B `12.640/58.589 ms`；Snapshot `4.461/7.541` 对 `4.665/7.946 ms`；PollEvents call→FULL `6.595/8.249` 对 `6.767/9.044 ms`。B 的聚合 Submit 虽在阈值内，两个子 shard 工具退出非零、整轮失败，不能用聚合统计掩盖子 shard 失败。

对 PID/TID/start ticks 匹配的 9 个完整采样区间，证据 writer 线程 CPU/累计 runqueue 等待由 A `228.8/1559.3 ms` 变为 B `254.4/1627.7 ms`；不同线程可重叠且此为单组诊断，不证明因果。该候选未表现正向收益，标为 `NOT_ADOPTED_WITH_EVIDENCE` 并保持默认关闭；未做三组、LOW/HOT 与完整故障矩阵，不得部署或宣称完成。两轮资源证据缺 cgroup I/O 与逐任务 block-I/O；证据 `/home/xuanling/tradecore-evidence-batch-f2837c1-{a1,b1}`。

### 2026-10-28：逐笔 FULL 边界与精确整数候选

对资源完整轮 `/home/xuanling/tradecore-resource-authorized-767b35b-r2` 的 6,992 次测量窗 PollEvents 逐条拆解，771 次 call→FULL ≥10 ms；其中 690 次以消费者 FULL commit 墙钟区间为 RPC、FULL、其他剩余区间中最长，63 次 RPC 最长，18 次剩余区间最长。这是同一请求的分段统计而非不同总体分位相减；剩余区间包括校验、SQL、调度及时间间隙，FULL 墙钟也不能等同物理设备耗时。当前证据不能将共享设备上 Journal、投影与消费者的影响完全归因或宣称 SSD 是唯一瓶颈。

既有 owner CPU profile 的 `require_nonnegative` 被调用 183,600 次，累计线程 CPU 约 0.164 秒。服务器 CPython 3.10.21、同核 8 组交错微测显示，对合法精确内置 `int` 直接做非负检查比原组合 `isinstance` 检查快；这是局部微测，存在频率/调度变化，不是端到端收益。新增默认关闭的 `TRADECORE_DIAGNOSTIC_BUILTIN_INT_VALIDATION=1` 候选：仅对合法、精确内置 `int` 直接完成等价正数/非负判断；bool、子类、无效值和其他输入完全回到原校验，不缓存账户、委托或其他值。该开关会被隔离候选的子进程继承，而非仅作用于 owner；必须用整组合对照判断影响。待 Linux 差分、真实进程故障和 LOW/HOT/HIGH 验收，不能先行部署或签发性能资格。

实际函数同核 8 组交错微测，每轮 150 万次：原实现 `0.1155–0.1187 s`，候选 `0.0879–0.0907 s`。Windows 两种开关状态分别 54 passed 和 11 项 subtest；Linux 两种状态分别 62 passed 和 11 项 subtest，覆盖非法值、bool、整数子类和比较异常的原语义。该定向回归不代替完整故障矩阵。

同一 revision `3ad96cd`、256 页预触节奏下，在隔离 no-Adapter 候选上做 A1/B1/B2/A2/A3/B3 交错 10 秒对照，A 关闭整数候选、B 开启；每轮 Submit 与 Snapshot 各 20,000 窗内完成，最终 40,800 条事件各自与 fresh Mock source/checkpoint/lifecycle 逐条闭合。第一对与其余轮使用不同的 sequence base，身份不相同，不能视为逐笔配对；下表仅为每轮独立分布，单位 ms：

| 轮次 | Submit P95/P99 | Snapshot P95/P99 | PollEvents call→FULL P95/P99 |
| --- | --- | --- | --- |
| A1 | 10.494 / 37.487 | 4.481 / 7.868 | 6.582 / 8.471 |
| B1 | 10.764 / 45.285 | 4.465 / 7.856 | 6.722 / 9.003 |
| B2 | 10.854 / 38.306 | 4.481 / 7.606 | 6.423 / 8.233 |
| A2 | 10.658 / 25.641 | 4.452 / 7.709 | 6.795 / 9.052 |
| A3 | 10.928 / 48.890 | 4.497 / 7.655 | 6.717 / 8.430 |
| B3 | 11.141 / 41.583 | 4.583 / 7.651 | 6.513 / 8.503 |

按线程身份和 9 个完整一秒采样区间配对，WRITE actor 累计 CPU：A1/B1 `4.519/4.515 s`、A2/B2 `4.531/4.520 s`、A3/B3 `4.508/4.492 s`；线程 runqueue 时间可互相重叠，不能作为逐笔耗时。候选 Submit P95 三组均未优于对应基线，P99 有升有降，微测优势没有转化为稳定的整链路收益，标记 `NOT_ADOPTED_WITH_EVIDENCE`，继续默认关闭。六轮缺 cgroup I/O 和逐任务 block-I/O 计数，资源证据 `complete=false`；短轮、两项未验收诊断组合与工具退出码均不能替代 LOW/HOT/故障/HIGH 正式门禁。证据 `/home/xuanling/tradecore-int-validation-3ad96cd-{a1,b1,b2,a2,a3,b3}`；正式服务未切换。

同一授权资源轮进一步按 monotonic 时钟对齐 PollEvents FULL 完成与完整的 59 个窗内资源采样区间。按 FULL 结束落入采样区间归组，最慢区间结束于窗起约 51.01 秒，42 次 FULL 的 P95 为 `97.954 ms`，候选 cgroup 当秒 `io.stat wbytes` 增量约 `314.5 MiB`，CPU 77 维护进程 PID 641388 的 `/proc/io write_bytes` 增量约 `256.0 MiB`；窗起约 43.01 秒分别为 56 次、`29.541 ms`、`305.8 MiB` 和 `231.4 MiB`。但写入量最高的约 26.01 秒 cgroup `608.6 MiB`、同进程 `256.0 MiB`，167 次 FULL 的 P95 仅 `4.550 ms`；约 18.01 秒分别为 `534.0 MiB`、`256.0 MiB`，182 次 FULL P95 `4.468 ms`。窗起约 48.01 秒 FULL P95 `12.550 ms`，cgroup 增量 `53.4 MiB`、该进程增量为零。这些观察表明高写入和 FULL 长尾有时重合、有时不重合，单凭写入量既不能证明因果，也不足以解释全部长尾。

以上是进程和 cgroup 的不同记账口径，不可相加或当作物理设备完成时间；FULL 按结束时刻归组，实际调用可能跨越采样边界。消费者 checkpoint 位于同根盘，但不属于候选服务 cgroup，未被上述 cgroup `io.stat` 单独覆盖；当前样本没有设备队列时延、逐 fsync syscall 时延或可信的完整进程角色映射，无法把 Journal、投影、消费者 checkpoint 对设备的贡献精确相加，亦不能据此降低 FULL 保证。要继续归因应在隔离候选中同步采集受限的维护 phase、投影 commit 与消费者 FULL 边界，并验证同钟和采样扰动；新一轮仍需有完整资源权限和 LOW/HOT/故障/HIGH 门禁，不能把此次相关性分析升级为验收。
