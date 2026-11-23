# Consumer UDS 长运行瓶颈优化实施与逐步验收方案

> 原始证据保留状态（2026-10-27）：用户明确授权删除旧Journal、投影库及失败证据，已清空`/var/lib/tradecore-longrun-qualification`和`/var/lib/tradecore-performance-candidates`中的历史测试子目录。本文历史性能数字及哈希保留为记录，涉及这两个根目录的原始证据已不再可用，不能将它们用于新的逐条复算或发布验收。清理与十二小时启动预检的当前状态见24号方案；十二小时测量仍未启动。

状态：`IMPLEMENTED_NOT_ACCEPTED`（S0测量、S1/S2候选、S3读取owner、S4事务消冗余及S7退出证据子包已推进；完整阶段及性能验收未完成）。本文保留实施合同，并在末尾记录逐包事实，不以局部测试替代整阶段验收。

适用基线：运行代码`fac5191`，三轮测量checkout为仅新增记录的`4a66b8f`，结果归档于`cb3aed6`。目标为纯Python实现的持久no-Adapter Consumer组合；现阶段生产LONG_RUNNING能力注册、交易READY和十二小时开跑门禁保持关闭。

相关文档：[长运行实施方案](./21-Consumer-UDS长运行优化实施与验收方案.md)、[真实实施与验收记录](./23-长运行功能包实施与验收记录.md)、[十二小时验收合同](./24-Consumer-UDS十二小时长运行压测与验收方案.md)。本文细化优化步骤，不替代上述长期容量、恢复或统一性能标准。

## 1 事实基线与优化边界

### 1.1 已证实的失败

| 项目 | R1 | R2 | R3 |
|---|---:|---:|---:|
| 30秒窗内提交完成 | 28,271 | 46,155 | 27,603 |
| 30秒窗内热页完成 | 28,269 | 46,153 | 27,601 |
| 计划每种操作 | 60,000 | 60,000 | 60,000 |
| 未发请求，两类合计 | 63,450 | 27,690 | 64,780 |
| Submit scheduled P95，ms | 346.292 | 321.913 | 356.516 |
| Submit发送完成→服务端首字节观察P95，ms | 136.256 | 131.012 | 138.211 |
| Submit schema后业务处理P95，ms | 0.253150 | 0.256955 | 0.252699 |
| 应有源事件 / 观察结束唯一FULL事件 | 57,342 / 38,886 | 93,110 / 37,028 | 56,006 / 38,640 |
| 最终投影 / 源LSN | 114,684 / 114,684 | 144,864 / 186,220 | 112,012 / 112,012 |

已发请求全部收到成功响应，但不能覆盖漏发。已消费部分source→FULL checkpoint的P95约31秒，仍有未消费事件，因此不是完整总体P95。三轮trace及客户端raw已逐请求配对并校验完整性；完整事实见实施记录，原始证据位于服务器`/var/lib/tradecore-longrun-qualification/mixed-r1`至`mixed-r3`。

当前fixture每单约4条fact、2个消费者事件；目标2,000订单/s需要约8,000 fact/s投影与4,000唯一事件/s消费。2,000热页QPS的每页有100条订单，不是2,000行/s。辅助操作、额外事实与准备数据另计，不能用固定倍率掩盖实际差异。

### 1.2 已知结构成本与待验证假设

- 主reactor、串行OMS owner与单投影写入进程仍是串行点；16个独占核不等于16路OMS并发写入。
- reactor的普通ready窗口为24，达到512连接后切换burst参数；该阈值与首帧等待是应验证的放大因素，尚未证明是全部尾延迟的单一原因。
- 父进程早期绑定主核，后台线程可能继承同核；需要同期线程亲和性及调度证据，不能把缺失采样当成某核100%。
- 投影是SQLite `DELETE＋FULL`，默认16个已到达batch合并；batch内仍有重复meta、编码、哈希与逐条SQL操作。
- 读取owner每次命令重新创建、校验及关闭只读数据库连接。
- 现有滚动消费者虽然使用4个签名分片，但每次step只进行一个RPC，再串行确认该页checkpoint。
- 未观察到OOM、触碰内存上限或CPUQuota节流；这不排除GIL、同核竞争、I/O、读写锁或发生器自身调度干扰。

## 2 不允许改变的合同

1. 仅使用项目`.venv`中的CPython 3.10；实现保持纯Python，不引入自研C++/Rust热路径。
2. 不新增Redis热路径，不以更换mmap技术栈作为本批默认路线。
3. 保留输入校验、SO_PEERCRED、binding/scope隔离、风控预留、幂等及Journal-first。
4. 订单状态、事件和watermark只在原有提交条件满足后可见；SQL及消费者checkpoint的FULL语义不降低。
5. 保留完整batch、CRC/hash、签名cursor、范围边界、代切换和交付前后健康检查；不伪造查询权威。
6. 队列、连接、字节、进程、FD、数据库与磁盘预算均有界。不得通过扩大缓冲、漏记请求、自动补发或隐藏超时过关。
7. 默认不改变GC、GIL间隔、不自旋抢占，不进行运行时CPU扩容。当前`dev`固定资源路线与独立混合自适应分支不能混用结论。
8. 部署只通过Git使用正式目录`/opt/xuanling-tradecore`，同时验证安装wheel；不以home候选实例代替正式部署。
9. 不重启服务器，不停止其他项目；任何额外CPU分区或主机IRQ变更须单独评审，不隐含在性能调参中。
10. 不启用真实EMT/QMT。真实EMT仍遵守500笔/秒及499 burst边界，本批Mock指标不能推广为柜台能力。

## 3 执行、状态与提交规则

### 3.1 阶段矩阵

| 阶段 | 目标 | 主要依赖 | 当前状态 |
|---|---|---|---|
| S0 | 测量、分类、共同窗口与发生器有效性 | 无 | 分类/共同窗口子包已实现，阶段未验收 |
| S1 | 首帧读取与有界公平调度 | S0 | 模块正确性通过；固定预算性能未通过，未采用 |
| S2 | 固定CPU角色与后台线程隔离 | S0；对S1做配对比较 | Linux/TID模块通过；HOT候选仍不达标，默认未采用 |
| S3 | 读取owner连接复用与短读事务 | S0 | 四种读owner命令及独立只读响应进程的功能/故障模块通过；候选默认关闭，完整性能组合未采用 |
| S4 | 事务消冗余、批处理及有界不可变seed复用 | S0、S3 | 最新Linux/kill/100K及隔离9K三轮通过；两轮混合投影lag<1秒，第三轮冷准入拒绝且HOT/端到端SLO未过，整体仍未验收 |
| S5 | 条件性WAL评估 | S3、S4及锁等待证据 | 条件阶段，未决策 |
| S6 | 有界签名分片并发消费 | S3、S4；若采用WAL则加S5 | S6A客户端checkpoint并发模块通过；服务端仍单权威cursor，性能组合未采用，阶段未验收 |
| S7 | 完整子进程退出与失败证据 | S0；最终组合须包含S1–S6选择 | Linux/独立CLI helper/小样本同根3次重启通过；完整故障与高压收尾待验收 |
| S8 | 正式部署、低压/高压/突发组合复验 | 前述必选阶段完成 | E4已执行30秒三轮诊断；完整性通过但延迟三轮不稳定，未进入60秒，待验收 |

实施顺序优先S0→S1→S2→S3→S4→S5决策→S6→S7→S8。S3可以独立开发，但不能提前跳过入口复测而声称总体达标。若S7发现正确性或进程归属缺陷，应前移修复，不等待吞吐优化。

### 3.2 每步完成条件

- 独立修改包完成后先执行单元/故障/兼容回归，归档Linux真实运行结果；Windows跳过Linux用例不能算Linux通过。
- 按每个功能包执行定向A/B，不要求每个小函数修改都重跑长稳；S8集中执行完整性能门禁。
- 每个独立修改包验收后提交、push到当前`dev`并核验远端commit。代码可按要求提交失败诊断，但其状态必须是`IMPLEMENTED_NOT_ACCEPTED`或`FAILED`，不得记为通过。
- 每步记录代码、安装payload、合同、配置、CPU/内存、数据规模及工具revision/hash。文档-only提交与运行payload关系必须说明。
- 状态仅用`DESIGNED`、`IMPLEMENTED_NOT_ACCEPTED`、`MODULE_ACCEPTED`、`COMBINATION_ACCEPTED`、`FAILED`及条件项`NOT_ADOPTED_WITH_EVIDENCE`。
- 模块正确性通过不等于2K混合性能通过；阶段性能改善不等于十二小时资格通过。

## 4 S0：补齐证据与发生器分类

### 4.1 修改位置与实施步骤

主要位置：`consumer/service_v2.py`、`consumer/request_trace_v2.py`、`read_model/process_order_projector.py`、`persistence/order_projection_store.py`、`tools/performance/consumer_uds_candidate_probe.py`、`consumer_uds_candidate_shards.py`、`consumer_uds_rolling_events_probe.py`及其`perfbench`证据模块。

1. 分开记录release队列失败、未发、建连失败、发送后未知、响应超时、业务拒绝、校验失败和证据写入失败。旧汇总位不再直接命名为传输失败。
2. 为完整计划账本保留planned/offered/completed/in-window/late/unissued，守恒规则覆盖全部计划。错误回包单列，不计成功吞吐。
3. 提交、查询和事件消费者使用同一已冻结的单调时钟T0/T1；准备/预热在T0前完成，窗后drain独立记录。不再以60秒观察覆盖30秒负载的旧诊断替代共同窗口验收。
4. 校验各进程clock identity、启动代和namespace；无法证明可比时钟时禁止跨进程延迟结论，不能以当前时钟反推历史样本。
5. reactor补充每轮accept/read/decode/dispatch/write计数、耗时和最老等待年龄；投影补充tail验证、reducer、编码、SQL、commit及控制通道等待；消费者拆RPC、解码、checkpoint SQL/FULL。
6. 所有新增采集通过有界缓冲异步输出；不在交易线程执行文件写入、fsync、外部命令或SQL采样。高成本perf/栈采样只用于明确标注的诊断轮。
7. 在同一绝对窗口采集线程TID/role/affinity、CPU/调度、上下文切换、缺页及块I/O等待，记录采样间隔和缺失。CPU/IRQ归因必须有同期证据。
8. 固定发生器资源并验证其发放能力。发生器目前核虽与服务不重叠但非内核独占；若其调度干扰影响发放，经评审后增加独占池或换核，并重新冻结基线，不擅改其他项目。

### 4.2 验收标准

- 构造每一类失败，稳定码分别命中；仅release失败时传输错误计数必须为0，整体性能仍FAIL。
- 已知N个计划的fixture中各终态分类守恒，缺报告/重复记录/未知分类不能默认成功；边界时刻与窗后完成逐条可核验。
- 所有成功测量请求均有客户端raw及服务trace配对；缺失、重复、负的同一因果链时间对必须使完整证据验收失败。合法跨边界先后差保留原值，不强制改正数。
- 故意提供不同clock identity、错revision、截断/改写segment和丢页证据，分析器必须拒绝全量结论。
- 投影每次FULL提交的真实batch/fact数及watermark有配对记录；无同期样本的CPU/I/O结论明确`NOT_MEASURED`。
- 新采集的建议开销目标为HOT P95增幅≤5%，采用相同full-trace配置交错三组A/B；该相对目标不替代HOT绝对SLO。若超标先降低采集实现开销，不能关闭必需证据后发布通过。
- 发生器对同合同的明确标注可控响应fixture完成各2K/s共同发放、call-start排队P99≤10ms、漏发0；此项只证明发生器，不证明TradeCore。随后真实TradeCore测试的排队/漏发仍全部计失败。

交付：分类测试、时钟/边界/证据故障测试、阶段指标schema、共同窗口运行器及冻结配置。回滚仅回退新增采集代码；旧失败报告保留原样，新分析结论另建文件。

## 5 S1：入口首帧读取与有界调度

### 5.1 修改位置与实施步骤

主要位置：`consumer/service_v2.py`、`fair_queue_v2.py`、`unix_v2.py`及reactor相关测试。

1. 先用S0确认最老已就绪首帧等待及readiness→recv阶段，不把accepted时间当内核数据到达时间。
2. 将accept、已就绪首帧读取、dispatch和响应发送分别设定固定数量/时间/字节预算，旧连接具有公平服务机会。
3. 比较当前24-ready/512连接切换与固定预算方案，冻结选型；不在测量中换参数，不引入运行时CPU/容量扩缩。
4. 保留单一socket/selector所有者、连接内顺序和OMS账户owner；不让多个线程同时读同一个socket或并发写同一账户状态。
5. 优先回收已完成连接、处理待发送响应，防止仅增大ready窗口后转移到输出拥塞。读取、输出与总连接容量均使用现有显式预算。

### 5.2 验收标准

- 覆盖fresh、persistent、半包/多包、半关闭、异常断开、慢发送/慢接收、部分写、EOF、超时、重用FD和并发stop，结果及资源归属正确。
- backlog及用户态容量上下界都测试；超载返回既有确定性分类，未知发送结果不得伪装未执行。无死锁、无限循环、重复dispatch或重复新单。
- 在普通/门槛/突发连接数两侧进行压力扫描，特别验证24、511、512、513等边界；仍须满足连接、ready、输出字节上限。
- 2K混合诊断中逐笔保留首帧等待；工程分段目标为已发送→完整帧观察P95≤5ms、P99≤10ms。它是新增内部工程目标，不代替最终scheduled SLO。
- HOT三轮及相同full-trace A/B无低压倒退；只有请求配对完整且队列/FD恢复后，才能记本阶段正确性通过。

交付：固定调度策略、预算/公平性不变量、边界与Linux回归、A/B分段结果。失败时回退该策略，不调大队列掩盖等待；保留未达目标结果。

## 6 S2：固定CPU角色与父进程后台隔离

### 6.1 修改位置与实施步骤

主要位置：`consumer/candidate_affinity.py`、`long_running_candidate.py`、对应正式systemd单位及受保护的partition helper。

1. 保留`tradecore.slice`的`64-79,160-175`整体独占集合及每服务内存预算。
2. 继续使用reactor64、projector65、cold66、sealer67、read owners68/69；新增父进程后台角色候选70/71。角色选择在启动前冻结。
3. 对线程和子进程分别设置启动亲和性并逐TID核验，不能只检查MainPID或AllowedCPUs。创建失败必须恢复调用线程原亲和性并清理已创建资源。
4. 对本项目DISARMED控制服务/监督器评估固定housekeeping核76/77及配对SMT，避免其与热核重叠；属于待实施本项目配置，不是当前已生效状态。
5. 不使用主核的SMT sibling承载另一计算热点；全部角色不得越出原分区，不修改外部进程或全局IRQ。

CPython 3.10的多线程不等于多核Python计算并行；迁核只能减少同核争用，不能据此承诺消除GIL。[官方线程说明](https://docs.python.org/3.10/library/threading.html)

### 6.2 验收标准

- 重复CPU、SMT同核、越界、缺核、非法类型及角色冲突在启动前拒绝，不创建半运行实例。
- Linux核验全部实际TID、worker PID及cgroup：主核、后台核、读写owner均在冻结布局内；其他用户态cgroup无独占池重叠。
- CPU热插拔/分区失效不能静默扩大允许集合；启动校验失败必须拒绝服务。无持续watchdog时明确该限制，不冒称运行中自动修复。
- 同revision默认布局与候选布局交错三组A/B，比较同窗首帧等待、上下文切换、HOT及混合SLO；不以CPU平均利用率更均匀作为采用理由。
- 若未证明收益或低压退化，布局标记`NOT_ADOPTED_WITH_EVIDENCE`，保留原布局；实际核数/服务配额若变更必须重建基线。

交付：布局配置、TID证明、启动/异常恢复测试和配对结果。回滚仅恢复本项目unit与角色配置；不回退内核独占为普通AllowedCPUs后仍声称独占。

## 7 S3：读取owner持久连接与短事务

### 7.1 修改位置与实施步骤

主要位置：`read_model/process_projection_reads.py`、`consumer_order_reader_v2.py`、`persistence/order_projection_store.py`及版本化快照读取模块。

1. 每个固定读取owner维护自己的只读连接，在正确线程内创建、使用和关闭，不跨进程/线程共享SQLite connection。
2. 连接建立时校验schema/profile/文件安全；每个请求仍独立开启/结束短读事务、读取新watermark并执行scope/byte/count检查。
3. 保留交付前后健康、stream代和generation校验；代切换或文件身份改变时关闭旧连接，显式切换，不误读旧代缓存。
4. 不将长期连接变成长事务，不为冻结快照pin住一个无限期SQL事务。
5. 错误/超时/IPC断开使owner失败或确定性重建；不透明重试后将未知结果当首次成功，不信任坏库缓存。

### 7.2 验收标准

- 相同owner连续至少10,000次成功只读命令只初始化一个可复用连接；代切换后恰好受控重建，未关闭句柄不累积。
- 覆盖跨binding读取、旧签名范围、落后watermark、坏hash/坏schema、文件替换/符号链接、查询超时及worker死亡，均按原合同拒绝。
- writer与reader并发时不误报合法提交为损坏；每次读事务结束后可观察后续提交，不能一直返回旧快照。
- 对旧实现进行固定数据回放差分，订单/事件/审计/分页边界与内容完全一致，无法证明完整性时不返回成功空页。
- 同窗记录open/check/query/close及锁等待，证明减少重复open成本；整组FD、进程、RSS/PSS保持预算内，HOT不退化。

交付：连接生命周期测试、代切换/故障测试、差分结果及分段A/B。失败回退到逐命令连接；本阶段不改变数据库文件格式。

## 8 S4：投影事务内消冗余与有界批处理

### 8.1 修改位置与实施步骤

主要位置：`persistence/order_projection_store.py`、`oms/order_projection.py`、`read_model/incremental_order_projector.py`、`process_order_projector.py`。

1. 在单个受限事务内复用meta、scope不变量、已解析订单checkpoint及相同编码/哈希；这些可变状态缓存只活到事务结束。2026-10-21经用户另行批准，允许默认关闭的跨事务不可变admission seed验证缓存：最多1,024个订单，编码bytes另限16MiB，单seed≤64KiB；仅冷投影owner使用，Journal/profile/投影generation namespace完全一致，命中重新核对当前seed SHA-256及完整bytes，未命中执行原canonical decode/rebuild验证。新增proof及LRU提升只在真实FULL commit返回后发布，失败整组不发布；每事务仍从当前SQL校验checkpoint hash、全部可变state/event/reservation、范围及幂等，绝不缓存当前订单状态或恢复权威。重启/close清空；仍受原6GiB进程资源限制，不更改256 batch／512 fact、FULL、原队列或等批规则。S4须通过原全部门禁，不能因允许缓存提前通过。
2. 同一订单跨多个batch仍按源顺序reducer，保留逐batch完整性/指纹/幂等验证。meta可在组内推进，但对外只随真实事务提交发布。
3. 对事实和独立索引写入采用适当的批量SQL，避免重复SQL往返；保留每张必要表和审计事实，不通过少写表获得成绩。
4. 比较16/32/64/128/256个已经到达的完整batch一次FULL事务。2026-10-21经用户明确批准，仅将事务batch上限由128修订为256，512 fact、原队列容量和FULL均不变；默认仍16，不自动采用256。按实际完整batch大小收集，不拆batch；至多保留一个由tail原有单batch上限约束的已校验lookahead，下一事务先处理它。没有新batch时立即提交，不等待凑批。进程退出后未提交lookahead从已持久cursor重读，不提前推进持久水位。
5. 选型同时考察低压等待、commit耗时、锁等待、投影lag与消费时延；不能只选最高投影TPS。

### 8.2 验收标准

- 旧/新实现对至少100,000订单、全部Mock状态及多个segment边界做流式差分，逻辑订单、facts/events、snapshot membership/version、watermark及回放结果一致。
- 覆盖同组同订单多次更新、重复batch、组中途坏CRC/坏hash/半batch、源缺口及预算耗尽；非法后一个batch必须使整组回滚，不对外发布前缀成功。
- 真实子进程在事务前/中/commit后被终止：恢复结果只能是完整已提交组或原前缀，不产生半组、重复事件或超过持久数据的watermark。
- 每个变体先做投影隔离穿透，再做真实2K混合；在目标fixture下具备持续≥8,000 fact/s的阶段能力，同时核验约4,000事件/s的实际放大，不能将单独投影通过替代组合通过。
- 隔离穿透的准备期producer book不得在投影计时进程中保留100K历史对象图：生产投影本来就在独立进程，数据源只需物理Journal。GC保持开启、阈值不变，记录gen2时间对；如保留producer图做诊断，须单独标识profile，与同fixture新基线对照，不能与projector-only结果混池。
- 采用配置必须在共同高压窗口不持续增加lag，符合原正常projection lag P99≤1秒要求；final source停止后30秒内追平，保持FULL，不能仅延长drain。
- HOT和低压不为等批增加延迟。若只有隔离吞吐上升而混合/消费退化，拒绝采用。

交付：事务差分、故障注入、批量参数A/B及实际commit指标。只允许已验证的读模型重建回滚，保留源Journal；不能删源或用旧writer直接打开未知格式。

## 9 S5：条件性SQLite WAL评估

此步骤不是默认切换。只有S0/S3/S4仍证明DELETE模式的读写互斥是主要等待，才实施WAL候选；否则记录锁等待证据和`NOT_ADOPTED_WITH_EVIDENCE`，不能记为“WAL已验收”。

### 9.1 实施要求

1. 核验项目`.venv`实际加载的SQLite版本、构建来源/hash及文件系统，不以系统sqlite命令版本代替。
2. 使用已修复相关WAL-reset问题的构建，例如SQLite≥3.51.3或可验证等价回移版本。WAL能改善读写并发，但仍需治理checkpoint与增长。[SQLite官方WAL说明](https://sqlite.org/wal.html)
3. 保持单writer和`synchronous=FULL`，将WAL/checkpoint/临时文件纳入统一磁盘和恢复预算；不改为NORMAL/OFF过关。
4. checkpoint由受控所有者执行，有明确容量水位、时间预算和失败处理；读者使用短事务，不允许长期读事务饿死回收。
5. 只在新建验收读模型代切换，旧代/源Journal保留。生产模式、备份与恢复规范需一并更新，不手工删除`-wal/-shm`。

### 9.2 验收标准

- 修复版本、实际动态库与配置可核验；不满足版本/构建证明则禁止启动候选模式。
- reader/writer并行、checkpoint交叉、reader死亡、writer崩溃、重启恢复、磁盘水位触发及checkpoint超时均有真实测试。
- FULL后成功状态与恢复内容一致；未完整提交不暴露；读模型与原源Journal重建结果一致。
- 完成至少30分钟目标读写与checkpoint交叉的有界增长验证，WAL及临时文件不越冻结配额、不永久增长；不能用30秒短轮宣称长期容量通过。
- 同资源、同数据规模、同同步语义对比DELETE与WAL；只有整体SLO改善、HOT不退化且故障/容量通过才采用。若未采用，S8仍测试最终选定DELETE配置。

回滚：先停止writer/readers，使用已验证流程checkpoint或从原Journal重建旧格式新代，校验后再启动旧代码。禁止只复制主数据库、丢弃其关联WAL后启动。

## 10 S6：有界签名分片并发消费

### 10.1 修改位置与实施步骤

主要位置：`tools/performance/perfbench/consumer_event_checkpoint.py`、`consumer_uds_rolling_events_probe.py`、必要的server replay接口和逐事件离线join工具。

1. 只使用服务端CreateReplayPartitions签发的正式分片；比较1/2/4个读取并发，同一分片最多一个在途请求。
2. 由单一checkpoint owner管理结果，先保留逐页FULL；每个分片严格按原cursor推进，重复回包单独识别，不重复计唯一事件。
3. 并发回包只能按各分片自己的前驱验证；全局frontier仅在全部已签范围覆盖后推进，不能用最快分片的LSN冒充全局进度。
4. 同时有界控制在途页数、事件数、字节、checkpoint排队和证据缓冲，不能从大页/大并发获得无限内存队列。
5. 若FULL本身已被证明为瓶颈，再单独评估已到达多页的原子合并事务；不等待凑页，不在FULL之前确认任何页，不偷换为异步checkpoint。
6. 每个源事件通过真实source commit区间与FULL完成时间配对；保留准备、负载及窗后终态分组，不仅筛选容易配对的中间区间。

### 10.2 验收标准

- 分片乱序完成、合法重复回包、过期/篡改token、跨scope token、缺页、错计数、缺事件、重启checkpoint和中途断连均有测试，不能绕过原签名校验。
- 同一分片无并行推进；进度检查要求所有已签分片耗尽且最终frontier覆盖实际源H。`pending_partitions=0`单独不足以通过。
- 2K提交fixture实际需要约4,000唯一事件/s，必须按实际源产出逐窗闭合，不能只达到2,000事件/s后宣称消费跟上。
- 最终源事件数=唯一FULL事件数，无不可解释缺口；重复事件另计不加吞吐。未消费事件不能从延迟统计中删除后发布总体P95。
- source commit→FULL分别输出下界和上界，性能通过使用保守上界；下界已超SLO可判FAIL，上界不达标或缺配对不能判PASS。
- 保持现定档HIGH消费checkpoint端到端P95≤10ms/P99≤40ms的严格口径，另报RPC→FULL子段及投影lag；较短子段不能替代端到端指标。
- 参数选择以完整混合SLO、低频公平性与有界资源为准；投影尚未跟上时不以增加读取并发掩盖源缺口。

交付：并发分片/单checkpoint实现、恶意/重启夹具、完整源与消费对账及1/2/4 A/B。失败回退单读取，但保留已FULL确认的原进度与原失败窗口，不重置token以洗掉缺口。

## 11 S7：完整退出、辅助进程与真实恢复

### 11.1 修改位置与实施步骤

主要位置：`consumer/long_running_candidate.py`、`candidate_systemd.py`、各`Process*`组件、lifecycle证书及离线校验器。

1. 将显式托管workers与解释器辅助进程分开枚举；记录PID、父子关系、启动代及退出状态，不能按模糊进程名终止其他项目。
2. 保留入口停止→在途请求有界结束→Mock收尾→固定最终source H→投影30秒drain→读取/冷存储/trace/source证据关闭的顺序，并核验实际资源归属。
3. 处理multiprocessing resource-tracker等解释器helper自然退出。若需CPython私有机制，必须有3.10实际实现兼容性检查、所有权证明及有界等待，不向嵌入式共享解释器全局停tracker。
4. lifecycle准确区分managed workers与全部owned OS后代；只有实际证明后才写成功，离线分析器拒绝旧字段被解释成更强证书。
5. 保留KillMode=mixed和Stop75秒，正常路径不依赖systemd最终SIGKILL清理；真实故障演练保留其非正常退出，而不是抹成普通成功。
6. 真实重启使用同一源/身份恢复新读模型代；部分初始化、损坏manifest和耗尽generation均fail-closed，不自动清库重建身份。

### 11.2 验收标准

- 正常负载停止：入口已关、accepted订单/事件闭合、projected=observed=source H、所有own workers及helper可核验正常退出、无systemd兜底强杀，证据完成后再发布成功。
- 暂停投影、变动source、worker退出非零、helper未退出、trace/source证据失败及超过30秒drain，全部返回非零且不发布全成功证书。
- 停止时间总和≤75秒；预算配置必须覆盖各子步骤，不能取消硬上限或延长时间把积压改判性能通过。
- 同一数据根受控重启至少三次：Journal与持久身份不变，活跃/终态/预留、冷幂等与签名cursor连续正确，代数在既有上限内；再次请求旧业务键不产生新订单。
- 不得为取得干净退出丢掉已接纳但未终态的订单；若不能闭合，保留持久源并明确失败/待恢复。
- 故意强杀与正常退出使用不同结果字段；systemd Result=success本身不构成证据通过。

交付：完整进程树与退出证书、故障矩阵、actual systemd日志及保留数据重启回归。故障时停止本实例并保留Journal/DB/证据，回滚代码不得自动重报未知副作用。

## 12 S8：正式部署与集中组合复验

### 12.1 部署前检查

1. Git fast-forward正式目录，工作区有冲突/脏改时停止；备份原unit/drop-ins、配置、revision和安装wheel到私有运维目录。
2. 安装wheel使用正确权限掩码，从`/tmp`以实际service用户验证导入，核对安装文件hash而非只看Git HEAD。
3. 验证根属主配置、精确UID/GID、socket目录权限、cgroup有效独占集合、所有worker/发生器配额、磁盘预算及无真实SDK。
4. 原BENCHMARK与qualification互斥；qualification保持人工启动，不enable未验收配置。测后恢复原服务并核验测试员访问。
5. 现有qualification RuntimeMaxSec=300只适用短诊断；更长LOW或60秒及其准备/收尾、后续MIX需要预先冻结合适的有限unit预算和运行器。禁止直接取消上限、修改duration即可冒充长稳能力。

### 12.2 集中验收矩阵

每个独立正式轮次在测量前受控初始化新namespace；一轮内不得reset、换进程或清Journal。正常恢复另用同一数据根，不用新namespace冒充恢复。

| 验收项 | 规模与强制通过标准 |
|---|---|
| Linux正确性/故障回归 | S0–S7必选功能及采用分支全部通过；skip明确说明，不代替Linux执行 |
| HOT | 三轮，每轮10,000正式样本、准备另计；全部成功，P95≤0.30ms/P99≤0.50ms |
| LOW | 同组合1/10/20 TPS分别统计；提交P95≤0.30ms/P99≤0.50ms，热页P95≤1ms/P99≤2ms；P99至少1,000样本，否则该分项证据未完成，不降为通过 |
| 30秒共同窗口 | 三轮各60,000提交＋60,000热页，均按计划发放并窗内成功完成；零漏发/技术错误，scheduled与发生器SLO满足下表 |
| 完整60秒HIGH | 三轮各120,000提交＋120,000热页，不能用两个有reset的30秒相加；相同生命周期/消费/trace全开 |
| HIGH延迟 | 提交scheduled P95≤20ms/P99≤60ms；热页及消费checkpoint端到端P95≤10ms/P99≤40ms；发生器call-start排队P99≤10ms |
| 低频背景公平性 | 持续HIGH中保留L/F小流，每流每操作P95≤10ms/P99≤20ms；不得靠总体均值覆盖饥饿 |
| 2K突发 | 同口径提交、热页及联合共同释放；每批全成功、整批<1秒，背景流量不中断；完整MIX的216批仍按24号文档执行，短轮不替代 |
| 降压恢复 | 计划降压后1秒内旧业务队列排空；L组提交和热页各在2秒内取得连续20个≤1ms响应；后续LOW持续稳定 |
| 受控过载 | 按冻结连接/字节/队列边界施压；资源有界、拒绝可分类、未知结果可核对，无崩溃、无重复新单，不要求超容量请求全接纳 |
| 投影与消费 | 实际fact/event倍率闭合、正常projection lag P99≤1秒；唯一FULL与源总量及实际H完全匹配，缺配对即不通过 |
| 资源 | 正常60秒内存P99≤4.5GiB，过程峰值≤5.1GiB；memory max/oom/oom_kill无增长；FD/线程/子进程/队列/字节不越冻结上限 |
| 证据/收尾 | trace与客户端raw完整配对、无drop/坏hash/错时钟；source/FULL逐事件对账；30秒投影drain、75秒总停止及S7完整进程退出通过 |

SHORT/BURST/HIGH、不同账户/策略/阶段、RPC与source→FULL分别统计；不能混池、相加总体分位数或用两组P95相减解释链路。无法按新共同窗口完整消费时明确FAIL，而不是套用旧60秒观察诊断的成功RPC次数。

### 12.3 结果与晋级

- S8全部通过后，仅记`PASS_SHORT_COMBINATION`；模块/LOW/30秒/60秒/突发/完整性每个结论单列，不能部分通过后统一写“已验收”。
- 十二小时仍须完成24号文档规定的完整600秒MIX运行器、三轮各3周期共90分钟、两小时资格、容量实测及未完成的SQL/Journal/trace治理。S5的30分钟WAL检查不能代替整套容量验收。
- 后续最长轨道仍区分MIX与STEADY；任何参数、运行payload、资源配额或schema变化都重跑受影响资格，不能沿用不同组合结果。
- 若本批完成仍不达2K，先依据完整stage证据决定是否需要独立进程入口或按明确ownership分区的投影架构；不得直接并发写同一OMS账户/SQLite库，不擅自改为C++或重新引入Redis。

## 13 证据、发布记录与失败回滚模板

每个阶段至少提交以下记录，敏感cursor/checkpoint目录保持私有，不把账号、密码、证券、价格或完整订单号写入公开报告：

| 字段 | 必须内容 |
|---|---|
| 身份 | stage、run_id、revision、安装payload/hash、合同/schema、配置digest、profile及数据规模 |
| 资源 | kernel/systemd/CPython/SQLite实际构建、CPU拓扑/有效独占集合/各TID角色、MemoryMax/TasksMax/FD和磁盘配额 |
| 测量 | T0/T1及clock identity、各计划分类计数、逐请求原始时间、HDR与真实P100、阶段分布和缺失 |
| 源与消费 | Journal ID、实际H、完整batch/fact/event计数、签名范围、FULL进度、重复/缺口及逐事件配对覆盖 |
| 退出 | 停止预算、managed/helper各自结果、systemd退出码、证据manifest/hash、恢复水位与身份连续性 |
| 结论 | 模块、阶段、组合、性能、恢复分别判定；失败原因、未测项、采用/未采用参数及回滚证据 |
| Git与部署 | commit、push及远端核验、正式安装/重启和原服务恢复结果；push失败不得写已推送 |

回滚只管理本项目精确服务和路径：停止入口并有界排空，保留持久事实/证据，恢复已备份unit/config和兼容wheel，核验数据格式与旧代码兼容性后启动；不兼容时从保留源构建新只读模型代。不能删除Journal、WAL、身份或坏证据强行让旧版本启动，也不能修改其他项目的服务。

最终交付必须明确回答：**改了什么、哪些正确性通过、哪些性能通过、未通过什么、当前实际运行哪个profile、能否进入下一门禁。** 除下列已记录子包外，其余阶段仍为设计状态；现有三轮失败不因代码或文档提交而改变。

## 14 逐包实施记录

### 2026-10-21 S0A：分类账本与共同窗口

- 拆分release拒绝、未尝试、已入队未开始、连接失败、发送未确认、发送后断连、响应超时、响应/业务/内容错误与证据写入失败。`transport_failed`不再复用全局停止位；release失败仍使性能FAIL。
- 每种操作记录planned/attempted/enqueued/offered/各结果/in-window/late，汇总重算并验证守恒；未知分类或缺证据不可通过。保留原报告，不改写历史结果。
- 分片发生器及事件消费者均可接受同一个`--start-ns`，准备/预热在前、测量T0/T1一致、窗后drain另计；增加clock identity和共同窗口校验函数，不把窗口一致当作源/FULL闭合证明。
- 修正旧测试fixture：准备等待使用非pinning的`GetOrderAuditTrail`，真实durable identity与冻结页字段必须存在，不放宽运行校验。
- 本地CPython 3.10定向测试：68 passed；push `3c2d580`后正式checkout Git更新、Linux同组68 passed（6.41秒），JUnit `/home/xuanling/tradecore-production-evidence/20261021-s0a.xml`。发生器2K/s fixture、采集分段及开销A/B尚待执行。S0整体保持`IMPLEMENTED_NOT_ACCEPTED`，S1–S8未晋级，LONG_RUNNING注册/READY/12h均未解除。
- 此子包只改压测工具/测试/本文，不更改服务wheel或热路径。Git-only更新后再执行Linux回归；实际部署与结果另记后续记录。

### 2026-10-21 S0B：共同窗口运行器及工具身份

- 新增`tools/performance/consumer_uds_common_window_probe.py`：以唯一未来30秒的T0启动提交/热页分片和事件消费者，固定相同T1；仅调用公共接口，不reset/启动服务/打开Adapter。CPU集合必须在既有grant内且互不重叠，进程等待及回收有上限。
- 子报告记录工具checkout revision；共同窗口校验拒绝缺失/冲突revision、clock identity、边界或clock类型。发生器汇总拒绝重复business namespace和计数被改写的子账本；旧缺字段报告保留但不能通过新门禁。
- 运行器明确`performance_status=NOT_EVALUATED`、`producer_join_status=NOT_MEASURED`，不把窗口匹配当作最终源闭合。窗后实际H/FULL、trace配对、资源和退出仍须独立校验。两类客户端共用外部有限发生器cgroup，不声明已为发生器实现内核独占。
- 新增负面用例：冲突CPU、子进程启动失败/超时回收、子报告失败保持失败；提交探针也显式拒绝非no-Adapter配置及越界规模，避免误将工具指向真实交易。
- 本地定向71 passed；push `32e1622`后正式checkout Git更新，Linux同组71 passed（6.86秒），JUnit `/home/xuanling/tradecore-production-evidence/20261021-s0b.xml`。没有更改业务热路径或wheel；S0整体仍未验收。

#### S0B 正式共同窗口失败诊断（不是性能通过）

证据根：`/var/lib/tradecore-longrun-qualification/s0b-common-r1`。工具`32e1622`，服务安装payload仍为原`fac5191`；固定原16物理核池及6GiB服务预算，发生器和消费者分别原3GiB限制、CPU不重叠。两客户端实际T0=`2730294805401427`、T1=`2730324805401427`，clock identity一致。原服务已恢复，未启动真实Adapter。

| 项目 | 结果 |
|---|---:|
| 每类计划 / 窗内成功 | 60,000 / 52,930 |
| 每类release拒绝 / 未尝试 | 5 / 7,065 |
| 两类未发合计 | 14,140 |
| 已发请求技术错误 / 入口超载拒绝 | 0 / 0 |
| Submit scheduled P95 / P99 | 326.369 / 349.176 ms |
| 热页 scheduled P95 / P99 | 324.534 / 346.817 ms |
| Submit发生器排队P99 | 219.283 ms |
| 事件消费者唯一FULL / RPC数 | 1,200 / 15 |
| 最终投影 / 实际源LSN | 51,534 / 213,320 |
| trace写入 / drop | 106,331 / 0 |

新分类正确区分了“release失败但传输无错误”，两操作账本守恒、共同窗口检查通过；吞吐/排队/SLO均FAIL。事件消费者`PUBLIC_RPC_OR_CHECKPOINT_FAILED`，并非全部事件已消费；最终投影30秒drain未追平，资格unit非零退出、没有成功lifecycle/source全量join。不能据此宣称消费者通过、完整总体延迟或十二小时资格。本轮仅一个诊断，不替代三轮门禁，也不把未测CPU归因成既定原因。

### 2026-10-21 S0C：可选有界内部阶段采集

- 增加`--stage-diagnostics`，默认关闭。reactor逐轮记录accept/read/decode/write/completion/select调用次数、累计/最大耗时、dispatch尝试及墙钟、最老已观察首帧等待；不是内核到达时间，也不把attempt当completed。
- 投影按真实SQL事务记录BEGIN/execute、显式FULL commit/rollback、reducer、编码、哈希、batch校验及已提交batch/fact/LSN范围；另记tail验证及父子控制通道等待。阶段为inclusive，不能直接相加；execute不含其后cursor取行成本。
- 采集由固定有界异步writer输出，stage单独子目录、schema/clock identity及hash-chain；队列/配额/最终status失败不能生成完整覆盖结论。启用时source占trace预算1/4、stage合计1/8、request余下5/8，不额外扩大总磁盘预算；关闭时维持原预算。
- 消费者增加实际FULL前后时间点及验证/SQL开始边界，重复页没有FULL时不伪造commit时间；检查点同步级别不变。
- 本地定向88 passed、15 Linux-only skipped，6 subtests passed；业务模块Pyright 0错误。push `8b8df46`后，正式checkout Linux同组103 passed、6 subtests passed（66.19秒），JUnit `/home/xuanling/tradecore-production-evidence/20261021-s0c.xml`，真实进程/UDS及诊断事务覆盖通过；安装wheel及开销A/B待执行。S0整体仍`IMPLEMENTED_NOT_ACCEPTED`，不可据此开启LONG_RUNNING/READY/12h。

### 2026-10-21 S0D：HOT真实时间对与持久Mock输入

- 修正HOT原始记录：以前起点取于构造请求前、结束时间却用该起点加内部调用耗时重建；现在保存同一次调用前、响应合同校验后的真实时间对，原始请求SHA可与服务trace关联，不修改历史证据。
- HOT对ACCEPTED必须验证请求关联、response/schema、正整数LSN及durable identity，错误回包不能仅凭状态字符串通过。summary分开`requests_passed`与`latency_slo_passed`，总passed必须同时满足HOT绝对SLO，不以全部成功代替延迟通过。
- 增加显式`--persistent-candidate --sequence-base`，只在LONG_RUNNING no-Adapter配置中使用既定`LR-<序号>`/H策略；预热及正式样本使用不重叠业务键。保留普通BENCHMARK调用方式，未自动修改服务或profile。
- 记录clock identity与工具revision，输出保持脱敏；本地32项定向通过。本包为测量正确性修正，尚不证明性能改善；下一步在正式安装后做同payload的采集关闭/开启配对比较。

#### S0C/S0D 正式安装与交错HOT诊断

`8ee1c00`正式Git checkout；新wheel SHA-256=`d3b08323e2bedc4cd18a3b64e536fda195629a034f8864fdcba923b4d763bab6`，7个运行文件与checkout逐文件hash相同，`xuanling`及`xuanling-tradecore`用户从`/tmp`隔离导入通过；旧wheel与unit/config备份位于`/var/lib/tradecore-deployments/20261021-s0c-8ee1c00`。S0D Linux32项通过（0.32秒）。三个正常服务恢复运行，qualification未enable，临时stage drop-in已撤除。

六轮均为独立namespace、固定原服务6GiB/16物理核池、发生器88/184及3GiB；同payload按off/on交错，每轮1,000预热＋10,000正式单在途请求，full request trace始终开启。证据`/var/lib/tradecore-longrun-qualification/s0c-hot-{off,on}-r{1,2,3}`；不是统一HOT最终资格或完整源消费闭合。

| 组 | off P50 / P95 / P99，ms | on P50 / P95 / P99，ms | 正式样本成功 |
|---|---|---|---|
| 1 | 0.263927 / 0.729988 / 3.553636 | 0.287072 / 0.660143 / 3.750713 | 两侧10,000/10,000 |
| 2 | 0.265009 / 0.918332 / 3.439323 | 0.290227 / 0.881446 / 3.824103 | 两侧10,000/10,000 |
| 3 | 0.261634 / 0.768756 / 3.418101 | 0.286311 / 0.597980 / 3.810632 | 两侧10,000/10,000 |

P95相对≤5%建议目标未被突破，但on的P50/P99有增加，且两侧绝对HOT SLO均FAIL；闭环单在途的负载节奏会随耗时改变，不能以P95降低证明采集“免费”或总体性能改善。六轮最终投影均未在30秒内追平44,000源facts，unit非零、未发布成功lifecycle。采集继续默认关闭，不能晋级发布或十二小时。

on三轮stage manifest/hash-chain完整、drop=0，记录真实FULL事务分别943/950/959个、已提交facts30,176/30,396/30,688，与最终投影LSN一致。`sql_commit`累计29,632/29,601/29,308 ms（均值约31ms）；reducer累计2,717/2,748/2,786ms，SQL execute约648/650/677ms，编码＋哈希约149/152/153ms，控制通道累计177～183ms。**这是commit调用包围区间，含I/O/锁/调度，不是已证明的物理SSD耗时**；execute不含之后取行，阶段inclusive，不能相加作为一条不重叠链路。主要阻塞区间已定位，仍需同期I/O/锁证据细化。

### 2026-10-21 S0E：明确标注的发生器fixture

- 增加独立`consumer_uds_generator_fixture.py`：受控AF_UNIX responder与同一分片发生器，只返回合成receipt及100行标准形状冻结页，不打开真实配置、Journal、账户或Adapter，也不实现真实cursor/replay/恢复。输出及各子报告明确`CONTROLLED_GENERATOR_FIXTURE`，TradeCore性能为`NOT_MEASURED`。
- 四个独立发生器event loop及独立响应CPU、连接/帧/任务/时间有界；外层必须施加有限cgroup。fixture不与真实TradeCore结果合并，共同窗口校验也拒绝fixture替代真实入口。
- 修复早唤醒边界：异步定时器返回后重新检查真实单调时钟，不提前发放、不将负排队时间夹为正样本；增加早唤醒用例。
- 本地46项定向通过；push `8d74715`后Linux同组46 passed（2.02秒），JUnit `/home/xuanling/tradecore-production-evidence/20261021-s0e.xml`。不能用合成应答证明OMS吞吐、Journal-first或柜台受理。

发生器外部非独占核首轮（证据`s0e-fixture-r1`）每类20,000次均成功、零漏发，但排队P99约15.2ms，门禁FAIL；原证据未改。随后只在本项目已有根独占池内划出夹具使用的5物理核：发生器72/73/74/75及其SMT168/169/170/171，响应器76；单个有限3GiB/Tasks128/cgroup，未扩占全机核或更改外部进程。短测期间暂停本项目三个无交易服务以避免同池竞争，finally恢复三个服务。此资源布局仅用于发生器fixture，不是新的生产服务布局，也不宣称IRQ隔离。

| 独占池内fixture | Submit / 热页窗内完成 | 漏发 / 拒绝 | Submit / 热页发放排队P99，ms |
|---|---|---|---|
| R1 | 20,000 / 20,000 | 0 / 0 | 2.185 / 2.193 |
| R2 | 20,000 / 20,000 | 0 / 0 | 3.615 / 3.623 |
| R3 | 20,000 / 20,000 | 0 / 0 | 2.351 / 2.359 |

证据根`/var/lib/tradecore-longrun-qualification/s0e-private-fixture-r1`至`r3`；每轮10秒、500/s×4 event loop，冷准备412次另计；每轮40,412应答、零拒绝、全部子进程正常退出。发生器夹具门禁三轮PASS，**仅证明该隔离资源下发生器能力**。不能据不同资源下的差异断言全部旧长尾均由外部CPU竞争导致；实际TradeCore后续组合须冻结无重叠布局并重新建立基线，不能沿用本表为真实吞吐通过。

### 2026-10-21 S0F：离线阶段校验与只读资源采样器

- 新增`analyze_consumer_stage_trace.py`，流式核验manifest/hash-chain、clock/journal、完整status、阶段时间合法性及fresh投影提交前缀；缺批、重复范围、回滚冒充成功或坏hash拒绝。单独输出“观察完整性”“源前缀是否追平”，不升级为恢复、性能或READY证书。
- SQL分位数标明为每个事务观察组的总耗时分布，不冒充单条SQL调用P95；同时输出真实调用总次数与单次最大值。完整trace生命周期统计不冒充冻结负载窗统计，安装wheel身份及客户端join不由该分析器证明。
- 新增`sample_consumer_resources.py`，仅允许读取本项目指定cgroup，支持同一T0及最多300秒、100ms～5秒固定间隔。记录每TID身份/亲和性、CPU/迁核/上下文切换、缺页、调度等待、每核IRQ/softIRQ及cgroup I/O；PID/TID复用、丢周期、配额或读取缺失显式记录，未知值不写0。
- 采样进程不修改CPU/IRQ/GC/内存/服务设置，raw流与128MiB上限及SHA另存；PSS/频率/物理syscall耗时未测。内核未启用任务delay accounting时明确NOT_MEASURED，不擅改全机sysctl。
- 本地12项定向通过；push `9416916`后Linux同组12 passed（0.66秒），JUnit `/home/xuanling/tradecore-production-evidence/20261021-s0f.xml`。三轮历史on数据经新分析器重算，clock/manifest/hash与fresh前缀合法，但相对真实H=44,000均未追平，未冒充源完整。
- 正式BENCHMARK cgroup只读闲态采样10秒，10/10样本、973,931字节，raw SHA-256=`d677763c42de6d99de2c84a649105060443e64ef9c03a4c6e20b25e2876cb660`；内核任务delay accounting未启用、该cgroup无`io.stat`，明确缺失，`complete=false`、CLI=1。证据在部署备份根`idle-resource-sample`，不是高压I/O证明，未修改全机配置。
- S0整体仍待最终共同窗口的源/trace配对及未完成证据收口，不宣称S1–S8或12小时通过。下列S1实现只用于可回退诊断，不以局部模块通过跳过S0证据门禁。

### 2026-10-21 S1A：启动冻结的有界预算候选与FD修复

- 新增`FixedReactorBudgets`：每轮accept最多32、receive/send各64次、receive/send字节各4MiB；accept/receive/send-syscall累计工作时间预算各1ms，dispatch最多24次/2ms，decoded ingress window为24。预算耗尽即让出，下轮重置，不等待凑量，不在线调参，不在512连接处切换25ms burst参数。单个不可抢占调用可能超过时间预算，不能声称硬实时。
- 接收预算按最大recv请求量保守预留（含被拒绝的辅助数据），不是实际收到字节；send只切片至本轮剩余字节；部分帧/部分响应继续由同一socket owner处理。全局连接/ready/output/冷查及read worker上限、原lane权重、OMS顺序与Journal-first不改变。24为读取窗口，不冒充异步resume/pipeline的全局硬容量。
- CLI新增仅候选入口的`--fixed-reactor-budgets`，默认关闭；保留原服务参数与旧调度作A/B，不将候选自动注册为生产LONG_RUNNING。trace侧记录实际预算使用与固定策略。
- 受控私有socketpair证实旧安装函数拒绝32个SCM_RIGHTS描述符后，进程FD从7增至23（遗留16个）；检查进程退出后已由内核释放，未向真实服务发送该输入。修复为拒绝前关闭全部可见接收FD，含截断消息前缀，加入8轮重复FD计数用例；拒绝规则没有放宽。
- 新增24/511/512/513连接边界、极小字节配额下半包/半关闭/有序回包、非法/不可变预算回归；完整候选恢复用例同时覆盖开启预算与stage采集。本地17 passed、57 Linux-only skipped、186 subtests passed，业务Pyright 0错误；Linux与同资源性能A/B仍待push后执行，状态`IMPLEMENTED_NOT_ACCEPTED`。

S1A首次Linux整体73 passed / 1 failed（37.18秒）：513连接用例同一测试进程同时持有客户端和服务端FD，触及CI进程软上限，产生EMFILE；正式服务LimitNOFILE=65536未修改。原JUnit `/home/xuanling/tradecore-production-evidence/20261021-s1a.xml`保留；该用例及极小字节用例独立复跑2 passed（0.73秒）不替代整体通过。修正测试fixture在已有hard allowance内临时声明4096软上限，使用unittest cleanup在service/peer关闭后恢复原值；hard不足直接失败，不跳过。下一次整体结果另建证据文件，未完成前仍不采用候选调度。

#### S1A 模块回归、正式安装与候选未采用

push `f00af1a`后Linux同组**74 passed、189 subtests passed**（37.25秒），新JUnit `/home/xuanling/tradecore-production-evidence/20261021-s1a-fd-envelope.xml`，原失败证据保留。正式Git checkout=`f00af1a`，运行代码修改为`71f7fcf`（之后仅测试/记录）；安装wheel SHA-256=`be356fefa0cf1d366492424856bcb6968c4bea3e8f1e8e2da2e6196c29311ec4`，9个运行文件hash一致，实际两个服务用户隔离导入通过。备份`/var/lib/tradecore-deployments/20261021-s1a-f00af1a`。已安装函数8轮私有socketpair核验FD始终7→7、零遗留。

仅做一组模块定向legacy/fixed诊断，证据`/var/lib/tradecore-longrun-qualification/s1a-{legacy,fixed}-r1`。同payload、full request/stage trace同开，原service16物理核池/6GiB，reactor64、owner65–69；发生器72–75及SMT/3GiB，事件观察76–77及SMT/3GiB。此轮为消除同池干扰暂时停本项目DISARMED控制服务及supervisor，finally恢复；**不是完整正常运行组合**，不与旧外部发生器布局混作A/B。

| 项目 | legacy | 固定预算候选 |
|---|---:|---:|
| 每类计划 | 60,000 | 60,000 |
| Submit / 热页成功 | 19,256 / 19,253 | 12,402 / 12,400 |
| 两类未发合计 | 81,491 | 95,198 |
| 已发raw / 成功配对trace | 38,509 / 38,509 | 24,802 / 24,802 |
| Submit scheduled P95 / P99，ms | 352.059 / 396.362 | 387.711 / 413.139 |
| Submit call→完整帧观察P95，ms | 149.425 | 150.389 |
| Submit frame→dispatch P95，ms | 12.809 | 15.841 |
| 热页frame→dispatch P95，ms | 4.780 | 2.801 |
| Submit schema后业务P95，ms | 0.237371 | 0.236690 |
| 实际源 / 最终投影LSN | 78,624 / 36,868 | 51,208 / 33,560 |
| 唯一FULL事件 / 消费者状态 | 1,678 / FAILED | 1,500 / FAILED |

计划窗30秒，但发生器在release失败后提前退出；表中延迟只针对已发子集，**不是完整30秒持续吞吐，更不是理论饱和TPS**。两轮缺少相同完整到达量，不能依据子集P95下降宣称收益或给出严格因果退化比例。客户端raw/hash与server manifest/clock逐条核验，已发成功请求无缺对/重复/反向因果时间；`call→frame`为保守上界，另保留`send-return→frame`有符号观察差，不冒充内核单程延迟。配对结果`client-server-pairing.json`。

结论：FD修复与调度正确性模块通过；候选未满足S1内部5/10ms目标或总体SLO，**不采用固定预算，保留原调度**；没有为过关扩大容量。投影/消费仍FAIL、资格unit非零且没有成功lifecycle，S7问题未解除。三个正常服务active/running、NRestarts=0，根独占集合仍`64-79,160-175`，临时drop-in已移除；`yangxue`以自身身份调用真实socket的ListAccounts=COMPLETED，仍为BENCHMARK/DISARMED/no-Adapter。S2–S8继续按原门禁推进，不以本模块回归代替发布验收。

### 2026-10-21 S2A：显式父进程后台线程角色

- 增加候选CLI `--background-cpus 70,71`（默认不启用）；启动前连同reactor/projector/cold/sealer/read owner核做同一physical/SMT、范围、类型与重复校验，仍在原16物理核分区内，不在线扩核。
- 可选模式下构造及start阶段的父进程后台线程继承后台集合；serve只将实际reactor线程固定到主核。特别处理在reactor内部才创建的4个deferred-read线程，创建期间临时切到后台集合后立即恢复reactor，避免它们仍继承64。
- 临时角色作用域在成功/异常后恢复调用线程原mask；构造失败也恢复原mask。设置后逐次核验实际mask，CPU缺失/授权缩减不能静默接受。默认未开启后台模式时保留旧成功构造后的布局；失败恢复修复不需要启用候选。
- 未更改全机IRQ、全局GIL/GC、其他进程或正常unit；本项目控制服务/supervisor housekeeping角色尚待A/B选择，不宣称已经部署。S1仍选旧策略，S2比较不能混入已拒绝的固定预算开关。
- 新增背景角色冲突/SMT、失败恢复、授权缩减及Linux实际TID/6个子owner亲和性用例；本地定向7 passed、18 Linux-only skipped，业务Pyright 0错误。代码子包待Git-only Linux回归及同资源交错A/B，阶段仍`IMPLEMENTED_NOT_ACCEPTED`。

#### S2A Linux、正式安装与三组HOT对照

push `7d0c402`后Linux定向36 passed（22.91秒），JUnit `/home/xuanling/tradecore-production-evidence/20261021-s2a.xml`。实际TID证明reactor、父后台线程（包括deferred read）、六个子owner按冻结角色分离；成功/异常恢复调用线程mask的用例通过。正式安装wheel SHA-256=`013802e9283f3db86a57f8fd080949219a7ad273358cd2ccded0f0e3faa19d19`，10个运行文件与checkout一致，两个实际服务用户隔离导入通过，备份`/var/lib/tradecore-deployments/20261021-s2a-7d0c402`。

证据`/var/lib/tradecore-longrun-qualification/s2a-hot-bg-{off,on}-r{1,2,3}`，总表位于上述部署备份的`hot-ab-summary.json`。同payload、request/stage trace均开启，旧reactor策略、reactor/子owner64–69；on仅增加父后台70/71。发生器72/168，服务6GiB、发生器3GiB，未扩原独占池。每轮1,000预热＋10,000单在途正式调用；为两侧公平隔离，暂时停本项目控制服务/supervisor，finally恢复，**不是完整正常组合验收**。

| 组 | off P50 / P95 / P99，ms | on P50 / P95 / P99，ms | 正式请求成功 |
|---|---|---|---|
| 1 | 0.286379 / 0.445266 / 4.099630 | 0.280151 / 0.469638 / 1.395157 | 各10,000/10,000 |
| 2 | 0.284729 / 0.498382 / 3.949412 | 0.281835 / 0.478283 / 1.386620 | 各10,000/10,000 |
| 3 | 0.286121 / 0.453147 / 3.879824 | 0.283498 / 0.467690 / 1.384621 | 各10,000/10,000 |

候选P99稳定下降，但P95并非三组均下降，且两侧均不满足HOT P95≤0.30/P99≤0.50ms；生成器均以SLO失败退出2，资格unit均非零退出1、未闭合投影30秒drain。不能据P99改善采用候选或解除S0/S8门禁。后台角色保持可选且默认关闭；控制服务housekeeping核配置尚未实施，完整混合/TID同期资源/源FULL闭合仍待完成。测后核验三个正常服务active、临时drop-in已移除，根独占集合`64-79,160-175`，13个其他顶层cgroup无重叠；未改变IRQ或其他项目。

### 2026-10-21 S3A：固定读取owner短事务连接复用候选

- 新增线程独占`ProjectionReadSession`，仅接受原四种固定命令（事件页、签名分片范围规划、订单查询、审计物理提示）；每个命令仍执行原短事务、最新watermark/meta、scope/行hash/byte/count校验，不缓存订单或数据库快照。
- 每次命令前后检查目录及数据库私有权限、symlink、dev/inode、schema cookie/user_version及预算；任何不安全变更关闭并毒化session，禁止自动重开。合法落后水位仍为STALE且连接可继续使用。显式新generation创建新owner；不能将原文件替换偷偷视为刷新。
- 增加候选开关`--reuse-projection-read-connections`，必须配置固定read CPUs、默认关闭。复用模式下UNAVAILABLE使父端owner失败，无透明重试；旧逐请求连接模式的失败合同保持原样。版本化快照专用reader尚未改用此session，本包不宣称覆盖全部快照读取。
- 新增10,000次命令只用一个连接、Linux FD不增长、后续提交可见、100订单并发writer/1,000次短读、跨scope/线程拒绝、schema/hash/meta及inode/权限/symlink故障；原IPC结果差分、scope/health fence及worker死亡扩为两种连接策略。本地13 passed、16 Linux-only skipped（22.96秒），Ruff/Pyright通过；Linux实际执行、连接分段A/B、完整性能仍待完成，状态`IMPLEMENTED_NOT_ACCEPTED`。不据此采用后台核布局、WAL或开启READY。

push `bbcf4c4`后正式checkout执行Linux **65 passed**（73.57秒），覆盖session、IPC owners、完整投影store、消费者读fence及候选服务回归；JUnit `/home/xuanling/tradecore-production-evidence/20261021-s3a.xml`。本包四种命令正确性记`MODULE_ACCEPTED`，不等于整个S3或性能通过。正式wheel SHA-256=`775dcbd8c38dbc9d17330452d22c55084845c587bd7754fbbfdb3f74fcbbc159`，11个运行文件hash与checkout相同，实际两个服务用户隔离导入通过；备份`/var/lib/tradecore-deployments/20261021-s3a-bbcf4c4`。三个正常服务恢复active，根独占集合不变。连接复用、后台核、固定reactor预算仍默认关闭；S3分段A/B与专用版本化快照reader未完成，未注册生产LONG_RUNNING或READY。

### 2026-10-21 S4A：仅事务内的有界消冗余候选

- 新增显式`--optimize-projection-transactions`，默认关闭、严格bool；跨候选/进程/增量projector传递。无需更改schema、DELETE/FULL、批数/fact上限、逐batch fingerprint/幂等及源顺序；不等待凑批。
- 一个`apply_many`内只读一次完整meta，将最终meta与全部必要表一起FULL提交；同组同订单的后续batch复用刚由原reducer生成的checkpoint bytes，最多128个键，正常/异常退出均清空，绝不跨事务缓存。原checkpoint解码与恢复reducer仍执行，不缓存未经验证的订单状态。
- 同一条event编码的SHA复用于orders/events/snapshot_versions；每个完整batch的fact INSERT使用executemany，仍写齐每条fact。所有batch审计anchor/hash、snapshot membership及中间version保留；坏后缀使整组回滚，不发布前缀。
- 新增16/32/64组大小、300订单含WORKING/REJECTED的逐表/分页差分、重复batch证明、meta SQL计数、坏hash/缺口/半batch/SQL故障整组回滚，以及Linux真实子进程在事务前/中/FULL后被kill的恢复用例。本地定向45 passed、11 Linux-only skipped（26.20秒），业务Pyright/Ruff通过。Linux kill、100,000订单全状态差分、8K fact/s与完整混合/lag/HOT尚待执行；本包`IMPLEMENTED_NOT_ACCEPTED`，不能据较小差分将S4整体标成通过。

S4A首次Linux65 passed / 3 failed（106.64秒），原JUnit `/home/xuanling/tradecore-production-evidence/20261021-s4a.xml`保留。三个kill用例在spawn参数序列化时遇到fact内部`mappingproxy`，尚未真正启动被测writer，因此不是事务恢复失败，也不能算kill通过。修正fixture只传冻结的实际源bound和segment路径，由子进程用原`SegmentedJournalTail`重新校验物理CRC/commit并生成batch，不改生产fact类型/序列化规则。修复后另建JUnit完整复跑。

push `f3c0982`后完整Linux **68 passed**（108.99秒），含三个真正启动writer后的kill边界、DELETE热journal恢复及integrity_check；JUnit `/home/xuanling/tradecore-production-evidence/20261021-s4a-physical-crash.xml`。S4A有界事务正确性子包`MODULE_ACCEPTED`，原失败保留，性能、规模和整个S4仍未通过。

### 2026-10-21 S4B：固定规模流式差分验收夹具

- 增加需显式`TRADECORE_PROJECTION_SCALE_ORDERS=100000`的100,000订单用例，普通回归明确skip、不冒充规模通过；用真实source、原CRC/commit tail、两个独立FULL库比较全部八张表、watermark和snapshot descriptor。源模型按既定Mock的80%FILLED/20%REJECTED、每单4 facts/2 events，不伪装交易SDK测试。
- verified batch队列最多64，逐表用有序cursor流式比较、不把全部SQL结果读入列表。合成producer仍保留固定100,000订单总体，因此外层必须限定CPU/6GiB内存/有限运行时间；这不是长期内存治理证明。100单smoke与普通定向本地9 passed、4显式skip（12.39秒）。规模/性能结果将在实际执行后另记。

正式checkout `773583a`，systemd有限独立单元`tradecore-s4b-diff-100k.service`，CPU74/170在原独占池内、MemoryMax6GiB、TasksMax128、RuntimeMax900秒，三个正常服务未停止。Linux **100,000订单、200,000物理batch、400,000 facts/200,000 events**，全部八表及watermark/快照membership流式差分通过（238.58秒），两个库均DELETE/FULL；无SDK/网络委托。JUnit `/home/xuanling/tradecore-production-evidence/20261021-s4b-diff-100k.xml`，新建隔离数据根`/var/lib/tradecore-longrun-qualification/s4b-diff-100k`保留。此模型只覆盖既定Mock的ACCEPTED→FILLED/REJECTED；其他状态由300单/既有生命周期回归覆盖，不能写成100,000订单覆盖所有交易状态。

同一测试内64-batch组按legacy后candidate顺序执行（不是交错三组性能A/B），累计apply耗时94.443秒/90.374秒。仅供量级诊断：约4,235/4,426 fact/s，不含source生成/尾读/逐表校验，**仍不足8,000 fact/s阶段目标**，更不能等同完整UDS吞吐。正常服务同期开启，未证明无外部I/O干扰，不能根据这4.3%差异采用候选。systemd输出的memory peak=2.1M与数据规模不符，标记该资源统计`NOT_VERIFIED`，不将其作为6GiB实测资源通过；只记录已设置的有限配额。S4B规模正确性子项通过，S4整体/持续能力/HOT/混合仍`IMPLEMENTED_NOT_ACCEPTED`，S5无充分锁等待证据，尚不切换WAL。

### 2026-10-21 S7A：前移修复helper所有权与过强退出证据

- 按§3允许前移正确性修复的规则，先修正v1的`all_children_exited_zero`仅验证托管worker却可能被解释为全部OS后代的问题。嵌入式`candidate.close()`只生成`managed-exit`证据，明确helper `NOT_VERIFIED`，不关闭pytest/上游解释器共享tracker，不再生成强lifecycle。
- 独立CLI在创建任何worker前建立CPython 3.10专用tracker ownership：必须单线程、无既有/继承tracker或子进程、属于自己经KillMode=mixed验证的唯一service cgroup。记录PID/PPID/start ticks、实际解释器、精确tracker程序及父写端/子读端pipe inode。cgroup扫描包含未知/孤儿成员及子组，不能只靠进程名或父子树假设。
- 全部托管worker关闭后才验证只剩本PID/helper，关闭**已证明归属的**写FD使helper自然EOF退出；锁等待各最多1秒，waitpid WNOHANG最多2秒，不调用不受限`_stop()`、不发kill信号、不停止其他项目或共享tracker。身份改变、未知成员、超时/非零退出均失败且不生成成功证书；systemd有限兜底仍作为失败收容而非验收成功。
- 仅正常CLI停止且实际helper退出0、cgroup只剩parent后生成`consumer-candidate-final-lifecycle-v2`；与managed证明、source/trace hash、boot/time namespace及真实退出时间关联。完整source/FULL分析器拒绝旧v1、managed-only、错误时钟、无ownership或强杀helper，不改写历史报告。嵌入式真实UDS集成仍验证逐事件配对，但不再伪装独立CLI完整退出证明。
- 本地已有定向30 passed、10 Linux-only skipped（24.67秒），新增共享tracker拒绝用例单独9 passed；业务Pyright通过。Linux及正式CLI正常/故障/同根三次重启尚待验收，S7整体`IMPLEMENTED_NOT_ACCEPTED`，本子包不代表S6/S8或12小时通过。

#### S7A Linux、正式wheel与实际同根重启

push `f3e9a63`后Linux **41 passed**（36.36秒），JUnit `/home/xuanling/tradecore-production-evidence/20261021-s7a.xml`，包含真实UDS/共享解释器集成、旧证书/坏helper拒绝。正式Git checkout同revision；wheel SHA-256=`585007c8eb07a06b4724ec75d7e37e536aecdc182c7b6f1ce6aa263eda0e6f97`，12个运行文件hash一致，两个实际服务用户从`/tmp`隔离导入通过，备份`/var/lib/tradecore-deployments/20261021-s7a-f3e9a63`。S3/S4候选开关及S1/S2性能候选继续默认关闭，不以安装代码等同采用性能配置。

独立qualification CLI在实际CPython3.10.21、原CPU角色64–69/6GiB及KillMode=mixed/Stop75秒下执行三次启动/停止，**同一数据根、不reset、不清Journal**。成功证据`/var/lib/tradecore-longrun-qualification/s7a-cli-restart-r3/restart-summary.json`，每次均有独立v2 lifecycle/helper退出身份、unit invocation日志和脱敏客户端身份hash/LSN；不是高压或HOT性能轮。事件观察器在既有分区72/168、独立有限3GiB/30秒unit中，只调用真实公共UDS。

| 同根周期 | Submit调用 / 订单身份 | 源H | 新增FULL事件 / 持久累计 | unit / helper退出 | stop至验证完成包围区间 |
|---|---|---:|---:|---|---:|
| 1 | 30次，新30单 | 120 | 60 / 60 | 0 / 0，无强杀 | 0.577秒 |
| 2 | 30次旧业务键，相同订单hash | 120 | 0 / 60 | 0 / 0，无强杀 | 0.362秒 |
| 3 | 30次旧业务键，相同订单hash | 120 | 0 / 60 | 0 / 0，无强杀 | 0.359秒 |

首次60个事件逐条source commit区间→真实FULL checkpoint配对完整，`MATCHED_FRESH_MOCK_SOURCE`、30个Mock订单闭合；后两次保留原checkpoint、旧签名cursor能读取合法空后缀，源H不增加，未重报。读取模型按既定恢复流程重建新代，持久Journal/订单身份及scope不变。三次cgroup仅余parent后才发布helper证明，日志无resource-tracker泄漏提示或systemd SIGKILL；最后三个正常服务恢复active，未启用qualification常驻或Adapter。

前两次操作夹具失败证据保留：`s7a-cli-restart-r1`事件观察器继承控制进程32/33 mask却请求72/168，被正确CPU门禁拒绝；`r2`错误要求幂等receipt必须保持**首次接纳LSN**，但原合同合法返回已存在订单的最新终态事件LSN，实际Journal/订单hash及H均未变。`r3`据原幂等合同校验身份、合法LSN范围、无新facts/events及旧cursor连续性，不修改生产receipt或降低不重复新单标准。

S7A正常helper退出和小样本同根三次重启子项`MODULE_ACCEPTED`。暂停/漂移/损坏/辅助进程异常等完整正式故障矩阵及高压≤75秒收尾仍待执行；不能把本表升级为S7整体、S8、自动恢复交易READY或十二小时通过。

### 本轮收口与后续门禁

代码及验收证据逐独立包提交/push `dev`；正式安装payload为`f3e9a63`，其后仅记录提交不改变wheel。当前正式常驻仍是BENCHMARK/DISARMED/no-Adapter，qualification人工短测后停止，全部历史失败与Journal保留。

尚未通过的必选项：S0最终共同窗口完整源/FULL/trace与缺失资源证据；S1入口内部5/10ms与整体SLO；S2正常控制服务housekeeping/完整混合选择；S3专用snapshot reader及分段性能；S4持续≥8K fact/s与lag/HOT/混合；S5锁等待前提和采用决策；S6并发签名分片消费；S7完整正式故障矩阵/高压停止；S8完整三轮HOT/LOW/30秒/60秒/2K burst组合。**本轮没有启动十二小时压测，也不声明所有优化完成。**

### 2026-10-21 S4C：事务内原reducer复用与sync诊断

在`d76795a`正式checkout对5,000合成订单差分执行`strace -f -ttt -T -yy`（仅fsync/fdatasync/fcntl）及cProfile，私有证据`/var/lib/tradecore-longrun-qualification/s4c-profile-r1`。这是高开销诊断，不作为吞吐或SLO验收。两个投影库及其目录的fsync共1,347次、累计11,376.476ms，单次P50=8.561ms/P95=10.001ms；fcntl累计14.766ms、无错误。典型FULL事务有journal→目录→journal→database四次fsync；commit包围累计11.274秒。此轮已直接定位sync等待，不把commit时间一概解释为物理SSD性能或推断高压读写锁情况；未修改journal mode/synchronous/主机存储设置。

- cProfile同时发现原reducer反复从其刚生成的checkpoint恢复OrderBook、重复解析/验证同一seed。本包仅在**同一个有界SQL事务内**保留单订单、no-writes projection book；首个持久checkpoint仍完整校验，每次源batch仍经原restore_committed_batch及全局范围/fingerprint校验。跨事务一律清除，不成为交易恢复权威。
- 其他订单的全局LSN间隔只在私有no-writes子类中推进，等价于原reducer的checkpoint watermark替换；不新增或放宽普通OrderBook恢复/实时API。错误后对象永久毒化，表/事实/中间版本及FULL原子性不变。
- 增加交错WORKING/部分成交/撤单/拒绝/冲突逐batch字节差分、种子校验次数（同组三次更新原5次→1次）、跨account/execution/journal/重复位置/容量拒绝，扩大组大小回归至既有128上限。128仅为原上限内的附加候选，不扩大128 batch/512 fact或队列容量。
- 本地74 passed、4显式Linux/规模skip（24.81秒），Ruff/Pyright通过；Linux故障、最新100K差分、16/32/64/128阶段对照及真实混合仍待完成。继续`IMPLEMENTED_NOT_ACCEPTED`，未开启候选默认值。

push `4cadf86`后Linux93 passed、1个未显式开启的100K规模用例skip（109.20秒），JUnit `/home/xuanling/tradecore-production-evidence/20261021-s4c.xml`；包含真实kill、既有恢复reducer及候选集成回归。正确性子包通过，性能仍未采用。新增S4隔离stage夹具：源在T0前完整准备，计时包含真实tail校验、全部SQL/reducer及FULL；逐事务保存真实时间/LSN范围、事实计数和1秒桶，使用同一个最终H核验完整性。与先前仅apply计时区分，不包含源生成，也不冒充UDS/消费者吞吐。30秒以上及≥8K平均能力为显式性能门禁；准备与资源单列。100单smoke本地通过，正式变体仍待执行。

### 2026-10-21 S4D：同资源tail→FULL变体扫描

正式checkout `ae9690c`，六个独立新数据根`/var/lib/tradecore-longrun-qualification/s4d-{16-legacy,16-optimized,32-optimized,64-optimized,128-optimized,128-legacy}`。原CPU池内74/170、每个有限6GiB/Tasks128/240秒unit，其他正常服务未停；每组10,000订单、40,000 facts、20,000 projected events，无Adapter。全部物理prefix、持久count、DELETE/FULL及终态断言通过；并非三组交错A/B、≥30秒持续能力或UDS吞吐通过。

| 批数 / 策略 | tail→FULL总秒数 | fact/s | projected event/s |
|---|---:|---:|---:|
| 16 / legacy | 46.352 | 862.97 | 431.49 |
| 16 / optimized | 43.487 | 919.81 | 459.91 |
| 32 / optimized | 23.803 | 1,680.47 | 840.24 |
| 64 / optimized | 15.062 | 2,655.71 | 1,327.85 |
| 128 / optimized | 9.812 | 4,076.62 | 2,038.31 |
| 128 / legacy | 11.596 | 3,449.58 | 1,724.79 |

汇总、实际cgroup/proc 100ms资源采样及日志位于`/var/lib/tradecore-deployments/20261021-s4d-stage`，各JUnit `/home/xuanling/tradecore-production-evidence/20261021-s4d-<variant>.xml`；每个根保留capacity.json/transactions.json真实时间与范围。进程VmHWM约183～199MB，cgroup实时采样peak约486～487MB、memory max/oom/oom_kill及CPU节流均0，实际mask74/170；解决了此前只看unit结束打印的错误memory peak统计，未抹去原NOT_VERIFIED记录。

扫描后只读iostat五秒观察：根分区Samsung 9100 PRO 2TB（ext4）设备util约92.6～93.5%、flush await约1.86～1.89ms；全机io.pressure full avg10约61%。这是**扫描后**的主机观察，不冒充该扫描的同期逐请求归因或纯硬盘极限，也不能据此停止其他项目。最大合法批量候选仍低于8K目标，持续/混合性能不可通过；未扩大128/512上限、未切WAL、未修改全机存储或IRQ。

### 2026-10-21 S4E/S4F：最新规模复核及经批准的256边界

`0cf20e0`在原CPU74/170、MemoryMax6GiB/Tasks128的独立有限unit复核100,000订单八表差分通过，物理batch200,000、facts400,000、events200,000；另一次真实tail→FULL的128-batch/100,000订单计时81.131秒，4,930.29 fact/s、2,465.15 projected event/s。该次显式8K门禁按预期FAIL，不把正确性通过混写为性能通过。证据`/var/lib/tradecore-deployments/20261021-s4e-validation`，数据根`s4e-latest-diff-100k`及`s4e-pipeline-128-100k`，JUnit分别以`20261021-s4e-`开头，原失败保留。

用户随后明确批准“允许扩至256 batch，512 fact不变”。S4F据此修订第8节和生产store/投影worker/候选CLI的边界；默认16不变，FULL、原队列、单tail batch、事务512 fact及故障条件不变，不等待凑批。生产投影和阶段容量测试共用`VerifiedProjectionGroups`，按实际完整batch计数，至多一个已校验lookahead用于避免跨512边界丢失/拆分。lookahead不是持久进度，当前事务FULL前不推进SQL cursor；停止/失败后的重启由已持久prefix重读。没有改变源Journal、Adapter或交易READY。

新增256 batch/512 fact精确边界、跨segment/少量立即提交、溢出lookahead保序及stop、损坏tail异常传播、257 batch及超512拒绝、组尾错误整组回滚；真实子进程kill三边界扩至256，100K差分可显式冻结256参数。本地67 passed、8个Linux/显式规模skip（34.03秒），业务Pyright/Ruff通过；Linux故障、256规模/持续能力、低压和真实UDS混合仍待执行，S4保持`IMPLEMENTED_NOT_ACCEPTED`。

push `d619d5d`后Linux **110 passed、2个显式规模skip**（45.81秒），真实256/512事务kill三个边界通过；独立显式256组100,000订单八表差分也通过。JUnit `/home/xuanling/tradecore-production-evidence/20261021-s4f-{linux-correctness,diff-256-100k}.xml`。同资源256短测10,000订单/40,000 facts计时6.096秒、6,562.19 fact/s；长测100,000订单/400,000 facts计时65.353秒、6,120.57 fact/s及3,060.29 projected event/s，显式8K门禁FAIL。raw、实际cgroup采样及原失败保留于`/var/lib/tradecore-deployments/20261021-s4f-validation`和各`s4f-pipeline-*`数据根。规模与恢复正确性通过不等于持续性能通过，正常服务仍使用先前安装payload。

### 2026-10-21 S4G：有界只读segment视图复用

代码复核发现追赶多个已提交segment时，原tail在每个batch都重新open/fstat/mmap/close后继segment。新增默认关闭的`reuse_segment_views`，只随投影消冗余候选启用；至多current＋successor两个只读映射，不新增队列、不扫描历史、不缓存事实历史。每次使用仍校验后继static header、activation CRC/commit、范围与前驱关系；activation身份变化立即fail-closed。已校验的首帧采用同一份immutable bytes解码结果，避免重复解码首帧，所有成员仍完整CRC/commit/ordinal验证，store重复指纹/范围/状态机验证仍保留。

增加300个batch跨3段仅3次打开的有界性测试、缓存后static/activation/CRC/commit破坏拒绝、候选旋转/恢复/成员破坏用例；100K差分同时比较原tail与复用tail的每一个完整batch。阶段容量夹具也显式启用同一候选tail，legacy维持原实现。定向本地40 passed、8显式Linux/规模skip（20.18秒），Ruff/Pyright通过；Linux故障、最新规模、持续性能及真实UDS混合仍待执行。此优化没有更改源writer或正常交易路径，未改FULL/DELETE或256/512边界。

push `65524f7`后Linux定向回归及包含双tail逐batch比较的100K/256差分通过。三个独立100K长窗口分别57.678 / 57.053 / 60.084秒，6,935.10 / 7,011.06 / 6,657.30 fact/s（projected event为其一半）；三轮显式8K门禁均FAIL。证据`/var/lib/tradecore-deployments/20261021-s4g-validation`和`s4g-pipeline-256-100k{,-r2,-r3}`，原结果保留。另一次5000单cProfile诊断位于`s4g-cpu-profile`，只作函数CPU归因，因探针开销且与正确性作业同期开启，不作为容量成绩。未切换journal模式、磁盘或其他项目任务。

### 2026-10-21 S4H：事务内不可变seed与物理anchor消冗余

只读tail候选对自己刚验证的上一完整帧保存一份4096字节immutable bytes及精确cursor；下一次仍重新读取并逐字节比较完整帧，任何变更直接fail-closed。新reader或不同cursor仍走原CRC/commit/schema完整恢复校验，不能信任其他reader的缓存；不缓存源历史。单个事务内的single-order book只对同一不可变admission对象复用原方法生成的seed bytes，跨事务重建，外来seed canonical校验和reducer状态校验完全保留。正常writer不调用冷checkpoint编码hook，未在发单热路径增加缓存或分支。

新增精确decode次数/真实前帧破坏/新reader原校验、seed编码复用及跨事务重新编码、原reducer checkpoint字节差分。本地seed/原恢复回归78 passed、7显式Linux/规模skip（26.92秒），最终tail/seed/容量定向29 passed、1显式规模skip（4.48秒），Ruff/Pyright通过。最初seed次数测试误将canonical验证所需的build计为可消除调用，已修正为保留验证＋首次编码共2次；未删除该验证。Linux、最新持续能力与真实UDS组合待执行，不提前标记S4通过。

push `da6f897`后Linux回归及100K/256双tail/八表差分通过；10K短测8,062.63 fact/s，但只有4.961秒，不能代替≥30秒门禁。100K持续53.330秒、7,500.44 fact/s、3,750.22 projected event/s，显式8K门禁仍FAIL。证据`/var/lib/tradecore-deployments/20261021-s4h-validation`与各`s4h-*`数据根，原失败保留；不采用短窗口最高值冒充持续能力。

### 2026-10-21 S4I：事务内snapshot scope/ordinal复用

在原有`BEGIN IMMEDIATE`和唯一writer lease内，只读一次本事务首次使用的snapshot scope counter；逐订单保留原admission ordinal，每条snapshot_id和snapshot_version仍完整写入，scope counter在事务末与meta及其他七表一起FULL发布。同组更新复用该事务自己刚插入/验证的ordinal，不跨事务缓存、不缓存全部历史、不同scope不可复用。最多256个相关键，正常/异常结束清空；512 fact、队列、8MiB SQLite cache及6GiB外层配额均未变。

五订单/十事件用例中scope SELECT由5次降至1次、scope更新由5次降至1次、ordinal SELECT由5次降至0次，十条version全部保留，三张快照表逐行一致。新增事务末scope写失败使整个组回滚的用例。定向本地54 passed、8显式Linux/规模skip（31.34秒），后加scope故障五变体5 passed（1.64秒），业务Pyright/Ruff通过；Linux、最新规模、持续性能及真实消费者验证待执行。

### 2026-10-21 S4J：隔离阶段的producer生命周期与GC证据修正

S4H原100K raw中正常事务中位约44ms，但多次最大约1,495ms；其完整1秒桶存在周期性0～3K谷值。同期CPU约46.85秒/53.33秒墙钟，不能把该停顿直接宣称物理SSD。代码检查发现`pipeline_capacity`在T0之后仍保留准备期100K订单的producer book，生产`ProcessOrderProjector`却是独立进程，没有这张producer历史对象图。

修正阶段fixture：源准备/封存仍在T0之前；只在准备期释放不再使用的producer book引用并完成一次GC，物理Journal、全部CRC/FULL/SQL工作不删减。计时期间GC始终开启且阈值不变，不disable/freeze或排除任意长尾；增加gen2 before/after时间对（最多128个，溢出即测量失败）。可通过显式`TRADECORE_PROJECTION_CAPACITY_RETAIN_PRODUCER_BOOK=1`保留原对象图对照，报告标识`PROJECTOR_WITH_RETAINED_PRODUCER_GRAPH`或`PREPARED_SOURCE_PROJECTOR_ONLY`，旧证据不重写、不与新profile混池。GC是否造成原停顿须等待该受控A/B，不凭周期性直接定因。

本包只修改容量夹具与测量合同，不改业务代码、GC配置、FULL、队列、cache或256/512上限；新的容量结论必须基于同fixture的原实现对照及真实UDS混合复核。

#### S4J 正式结果与安装（`e7f7b72`）

Linux **134 passed、2显式规模skip**（49.76秒）；此前`9b9d2b6`的100K/256延续正确性差分1 passed（164.70秒），最新业务hash未因J的夹具修改而改变。保留producer图的受控对照53.155秒、7,525.11 fact/s，6次gen2共9.024秒；projector-only三轮均保持GC开启及`700/10/10`阈值：

| 三轮100K订单 | 时间，秒 | fact/s | projected event/s | 完整1秒桶最小facts |
|---|---:|---:|---:|---:|
| R1 | 45.397 | 8,811.17 | 4,405.59 | 8,192 |
| R2 | 45.522 | 8,786.88 | 4,393.44 | 8,192 |
| R3 | 45.096 | 8,869.87 | 4,434.93 | 8,704 |

三轮均400,000 facts/200,000 events、256/512、DELETE/FULL，所有完整1秒桶≥8K；每轮95次gen2共约1.19～1.22秒，没有排除长尾。CPU74/170、MemoryMax6GiB，实际cgroup峰值约3.52～3.54GiB，memory max/oom/oom_kill均0。同fixture原实现128组单轮81.037秒、4,936.04 fact/s；不是三组交错性能A/B，不能据此给出普遍提速比例。额外CPU65单逻辑核的同fixture100K复核45.660秒、8,760.42 fact/s；CPU67/163额外对照9,029.10 fact/s，不将67误标为projector核（实际role映射64=reactor、65=projector、66=cold、67=segment，68/69=read）。

证据`/var/lib/tradecore-deployments/20261021-s4j-validation`及`s4j-*`、`s4cpu65-projector-65-100k`数据根。已正式Git-only部署wheel，SHA-256=`f75bb062a94fb1318dbfc69925f8199204383163cf3a2372dc2f1ea3e23afe0d`，16个运行文件hash一致、两个实际服务用户隔离导入通过。备份`/var/lib/tradecore-deployments/20261021-s4-e7f7b72`；三个正常服务恢复，候选开关仍默认关闭。

#### 实际公共UDS混合与HOT：完整性通过，性能不通过

同一wheel、原16物理核独占池内部划分：服务64–71及SMT160–167（8物理核）、发生器72–75及SMT168–171（4物理核）、消费76/77及SMT172/173（2物理核），共14物理核、未扩占。服务MemoryMax6GiB、两个外部负载组各3GiB；不与先前资源布局直接混池。只运行no-Adapter Mock，正常控制服务仅在本项目有限窗口暂停并finally恢复，没有改其他项目或IRQ。

| 30秒共同窗口 | 详细stage on | stage off |
|---|---:|---:|
| 每类planned / offered / 最终成功 | 60,000 / 60,000 / 60,000 | 60,000 / 60,000 / 60,000 |
| 窗内Submit / 热页完成 | 59,786 / 59,799 | 59,962 / 59,972 |
| Submit scheduled P95 / P99，ms | 145.097 / 158.466 | 52.756 / 73.466 |
| 热页scheduled P95 / P99，ms | 143.655 / 157.549 | 46.399 / 67.109 |
| 漏发 / 技术错误 | 0 / 0 | 0 / 0 |
| 最终source / projection LSN | 241,600 / 241,600 | 241,600 / 241,600 |
| source→消费者FULL配对 | 120,800 / 120,800 | 120,800 / 120,800 |
| source→消费者FULL P99，秒 | 25.183 | 11.652 |
| 最终unit / 全owned进程退出 | success / 已验证 | success / 已验证 |

两轮均`MATCHED_FRESH_MOCK_SOURCE`：60,400订单、241,600 facts、120,800唯一FULL事件和最终生命周期完全闭合。这是本fixture完整性通过，**不是性能通过**。on完整stage hash-chain/LSN前缀校验通过，源120,800个完整batch逐个对应真实FULL事务；source→投影事务end保守上界P99约7.294秒、transaction-start下界P99约7.227秒，最终源停止后约7.340秒投影追平。off的已发布health采样也出现最老待投影源batch至少4.369秒的等待下界，不能只归因于详细采集开销；该采样不是完整逐事件P99证书。原数据在`s4-uds-{on,offdiag}-r1`，原FAIL保留。

六轮交错HOT（每轮1000预热＋10,000单在途）全部请求成功且unit/lifecycle成功；off/on P95分别为0.519609/0.498227、0.505047/0.522314、0.510576/0.526029ms，三组P95相对增加未超过5%，但P99各轮1.393～2.623ms，绝对HOT门禁仍FAIL，不能据P95宣称HOT已通过。数据`s4-hot-opt-{off,on}-r{1,2,3}`。未运行LOW完整门禁或12h。

### 2026-10-21 S4K：真实源拓扑与首个持久checkpoint消冗余

补充只读重放**实际公共UDS**的保留源（不是再生成配对订单）：核65、256/512、FULL、新建独立SQL，只读源段前后全文件SHA一致，60,400订单/120,800 batch/241,600 facts与最终生命周期闭合。原`e7f7b72`计时32.357秒、7,466.68 fact/s；同事务重复订单batch仅32,834（27.18%），而原配对夹具接近50%。证据`s4-actual-source-before-k/actual-capacity.json`，仅作真实payload/顺序诊断，不能把隔离配对夹具8.8K当作实际源已达标。

代码发现candidate首次加载持久checkpoint时，先自行解码seed校验归属，随后原`restore_recovery_checkpoint`又完整解码校验同一seed。K保留原restore的canonical、状态、事件和reservation全部校验，仅将归属检查移至恢复后的已验证single-order view，消除前置重复解码；新/旧scope仍拒绝，失败永久poison，参考reducer保持原路径。增加有持久前驱时decode从2次到1次、错误account/order拒绝，以及全量admission后才terminal的deferred差分模式（避免只覆盖同组配对）。本地原恢复/投影回归83 passed、7显式Linux/规模skip（31.10秒），随后含deferred差分定向25 passed、7显式Linux/规模skip（40.43秒），Ruff/Pyright通过；Linux、最坏顺序100K、真实源性能和混合重测待执行。

push `dfb98d0`后Linux定向回归和显式100K deferred/256八表差分均通过，故障/错误scope不退化。对同一份实际UDS源重放29.619秒、8,156.88 fact/s，源段前后SHA不变，全部逻辑计数一致；因不足30秒，仅是实际payload改进诊断，不当作持续资格。Git-only正式wheel SHA-256=`897dcddd2a5135fc7cec3f5a6c22d28ca64440cf2e3a04607bb6d26a89563bd8`、16文件hash和双服务用户隔离导入通过，备份`/var/lib/tradecore-deployments/20261021-s4k-dfb98d0`。

`s4k-uds-offdiag-r1`重新执行相同30秒2K＋2K公共UDS：每类60,000全发/最终成功、零漏发/技术错误；窗内Submit59,971/热页59,982，scheduled P95/P99分别47.743/61.309及41.615/54.854ms，窗口/SLO仍FAIL。120,800唯一FULL事件、241,600源facts和60,400订单完整闭合，最终unit与全部owned生命周期success。完整消费者source→FULL P99约10.461秒；已发布health采样最老待投影源batch等待下界最大2.808秒（原off为4.369秒），不是完整事件P99证书，也不能认作≤1秒通过。继续保持S4整体`IMPLEMENTED_NOT_ACCEPTED`。

### 2026-10-21 S4L：事务级facts与最终latest row批量SQL

不再对每个小源batch单独执行facts executemany，而是把原事务内≤512条facts一次批量写入；同订单的`orders`表本来只表示latest，故在该事务内仅写最终一行（≤256个键）。**每个物理batch、每条fact/event、每个snapshot version与membership仍完整保存**，不会合并/丢掉中间状态的历史。所有fingerprint/幂等/范围/原reducer校验仍逐batch执行；meta/scope、orders/facts和其余表在同一原FULL事务发布，没有跨事务缓存、凑批等待或新增入口队列。

新的事务局部行缓冲受既有256 batch/512 fact输入上限约束，normal/exception退出均清空，外层6GiB/数据库/磁盘限额不变。五单十事件用例验证仅两次bulk调用（5个最终order row、20条完整facts），十条snapshot version保留并与参考逐行相同；新增orders/facts批量flush失败整组回滚。定向本地56 passed、7显式Linux/规模skip（27.79秒），故障7变体及bulk精确计数另行通过，业务Pyright/Ruff通过；Linux、deferred100K、实际源与真实混合待验证。

push `e63da56`后Linux定向及显式deferred100K/256八表差分均通过；同一份保留真实源只读重放30.052秒、8,039.35 fact/s（`s4-actual-source-after-l`），与K的单轮8,156.88差异不足以证明性能更好或更差，未据此自动采用候选。原源段SHA一致，没有删除源事实或改写失败记录。L正确性子包通过，整体S4仍未通过。

### 2026-10-21 S4M：复用原canonical round-trip的输出bytes

`OmsRecoverySeedV1.decode_and_verify`本来就会完整重建seed并要求输出bytes与输入逐字节一致。M仅在该原校验及scope恢复成功**之后**，将它证明的同一份immutable bytes交给当前no-write single-order book的checkpoint导出使用，不再额外build一次相同的admission；seed canonical校验、state/event/reservation校验没有删除。缓存仍只活在原SQL事务内，恢复新的事务仍重新验证；single-account/order、bytes预算、缓存身份变化保护及失败poison保留。

测试验证canonical build仍执行1次（此前另有1次重复checkpoint build）、新事务仍重新校验/编码，checkpoint与原参考逐字节相同；新增源seed及持久前驱seed破坏时必须在缓存赋值前失败，随后无法复用该book。原恢复/投影本地86 passed、7显式Linux/规模skip（28.08秒），新增负面用例在内的seed定向13 passed（1.94秒），Ruff/Pyright通过；最新Linux、deferred100K和真实混合待验证。

push `03ee48e`后Linux定向及100K deferred/256八表差分均通过；同一真实源重放29.020秒、8,325.40 fact/s，完整计数/源SHA不变。仍因不足30秒仅作诊断，不升级持续/混合资格。数据根`s4-actual-source-after-m`，源仍为保留的`s4-uds-offdiag-r1`，未重写旧源或旧结果。

### 2026-10-21 S4N：仅FULL事务边界的有界证据

增加默认关闭的`--projection-commit-diagnostics`，与原`--stage-diagnostics`互斥。每个真实SQL事务只记录BEGIN前/完整commit返回后的范围，以及真实commit/rollback调用包围时间；不为每次SQL、reducer、编码、hash、tail或source-control安装逐操作计时器。沿用原有4096条异步stage队列及现有总trace配额内的预算，未扩队列/磁盘；正常默认模式不变。

提交成功才发布真实committed batch/fact/LSN，回滚记录committed=0；stage的完整hash-chain、clock identity、关闭/丢失及子进程退出仍须通过。它可与source时间区间逐batch配对，transaction-end只是FULL返回的保守上界，不冒充纯SSD延迟；模块测量开销及真实混合仍需单独验收。

本地提交/rollback、逐表等价、缺sink拒绝、实际spawn进程的完整范围和关闭回归59 passed、15 Linux/显式规模skip（38.85秒），业务Pyright/Ruff通过。初始测试误要求Windows短事务严格正时间，已按原分析器允许的单调时钟等端点修正，未伪造时间或减少Linux性能标准。Linux及同payload轻量/关闭模式A/B、真实混合逐事件lag仍待执行；S4未提前验收。

N后续：`bdd802c`在Linux修正runner的既有HDR依赖路径后回归通过；正式wheel SHA-256=`85ee91838df00a181dc41fa1676d8fd707d6e8beaa7ce7f66094d6833ca6030e`，16个runtime文件及两服务用户隔离导入一致。30秒真实公共UDS关闭探针/仅commit探针两轮均每类60,000全发、最终成功，零漏发/技术错误，241,600源facts与120,800唯一FULL事件完整闭合；但关闭探针的Submit/Snapshot scheduled P95分别55.411/48.726ms，commit探针63.406/56.852ms，不能宣称探针开销门禁通过。完整source→事务证据覆盖483个FULL事务/120,800源batch，窗口投影lag P99下界2.411725秒、上界2.468348秒，最终source到投影结束2.423138秒；因此≤1秒要求明确FAIL，不将最终追平误写为窗口通过。原数据根`s4n-uds-offdiag-r1`、`s4n-uds-commit-r1`及其`s4-projection-analysis.json`保留。

### 2026-10-21 S4O：用户批准的有界不可变seed验证缓存

`81acf94`实现默认关闭的`--reuse-verified-admission-seeds`，要求同时开启事务消冗余。缓存仅包含原canonical decoder验证过的不可变admission DTO及原bytes/hash，最多1,024订单、编码16MiB；namespace绑定Journal/profile及本次投影root，单owner线程，重启不持久化。每次命中仍计算当前seed完整SHA并逐字节比较；可变订单状态、checkpoint checksum、事件、reservation及每个物理batch证明仍按原路径验证。新增proof和LRU提升只在FULL返回后发布，异常整组abort；新增FULL已完成/缓存尚未发布的实际kill边界，恢复仍以数据库完整前缀为准，不依赖缓存。

本地新增缓存定向5 passed（3.51秒），原投影/恢复/进程组合73 passed、15 Linux/显式规模skip（26.97秒），业务Pyright/Ruff通过并push核验。缓存碰撞bytes、namespace/profile/thread、上限/驱逐、失败不发布及缓存命中仍拒绝非法可变checkpoint均有用例。规模夹具增加显式seed-cache开关；隔离容量夹具报告实际缓存数/bytes/hit/miss，并保持原GC/CPU/内存/FULL/源范围要求。Linux、10万订单、隔离及真实混合结果必须另行归档，不因该实现完成提前通过S4。

后续Linux 193 passed、2显式规模skip，实际kill含FULL返回后/缓存发布前边界；100K deferred八表差分通过。正式部署`9de539a`，wheel SHA-256=`f0e36aaefd89a36d589981a9ca9f3c72c26ce6e0cfdc00312f7cd20dadef49a2`，17文件及双服务用户校验一致。CPU65、6GiB、GC开启、FULL/256/512、每轮100K订单/400K facts三轮projector-only分别45.667/45.452/45.688秒，8,759.04/8,800.44/8,755.00 fact/s；每轮缓存1,024单/2,105,344编码bytes。该paired源同组复用使跨事务hit=0，不能拿这三轮证明真实payload缓存收益。

同commit级探针30秒真实2K＋2K对照：关闭缓存根`s4o-uds-cache-off-r1`，窗口投影lag P99上下界1.717567/1.771045秒；开启根`s4o-uds-cache-on-r1`为1.570767/1.623196秒，最终source→投影结束1.583316秒。开启缓存累计26,721 hit/60,400 miss，最大1,024单/2,234,368编码bytes；两轮120,800源batch全部配对、241,600 facts/120,800唯一FULL事件完整闭合，最终unit/lifecycle成功。开缓存Submit与热页scheduled P95分别47.677/41.615ms，仍有6条Submit窗后完成，客户端SLO及≤1秒投影门禁均FAIL。单对照不足宣称稳定改善，不提前采用到正常服务；S4整体仍`IMPLEMENTED_NOT_ACCEPTED`。

### 2026-10-21 S4P：完整events与snapshot versions事务批量写入

在原S4批量SQL范围内，将本事务≤256条events和≤256条snapshot versions改为各一次executemany，与已实现的facts/latest orders同一个FULL事务写入。缓冲仅包含原256 batch／512 fact里已经生成的行，事务结束/错误即清空，不是新入口队列；每个事件与每个快照版本均保留，批次指纹和source范围仍逐条验证并写入，watermark仍随原FULL发布。没有扩大缓存、外层内存或数据库容量，没有新增等待凑批。新增两个flush点失败回滚及缓存开/关两种故障组合，实际kill/八表规模差分、Linux及混合性能需对最新版本重新归档。

`5707921`本地79 passed、9 Linux/显式规模skip（30.52秒），Pyright/Ruff通过；push后Linux 206 passed、2显式规模skip（78.94秒），100K deferred/256/seed-cache八表差分通过。其性能仍待最新组合验证，不将module正确性直接写为S4通过。

### 2026-10-21 S4Q：单个有界读取组的保守source fence

旧完整探针轮`s4-uds-on-r1`有4,369次BOUND控制调用，包围耗时总计1.014058秒、最大18.147486ms；仅是该轮的诊断，不外推最新探针关闭轮的耗时。投影隔离源没有跨进程控制等待，所以必须独立检验这个差异。

事务消冗余候选在收集单组≤256 batch／512 fact时固定一次可信的已提交源前缀；新提交仅归入下一组。该window在SQL事务开始前结束，不延长SQL读事务或等待未来数据；原逐帧CRC/commit、activation链、归属、resume anchor、全batch指纹及SQL幂等/范围仍逐条核验。scope/segment容量或源前缀回退拒绝，读取异常清空window且poison；stop不再请求新window。没有新增历史缓存、控制通道、CPU或队列，默认非优化路径不变。容量夹具使用同一group实现；本地跨段/单水位调用数、当前window不吞新提交、非法scope、窗口异常和真实frame字节破坏都有用例，Linux及真实混合仍须验证。

### 2026-10-21 S4最新验收收口：`769e26f`，整体未通过

**代码与部署。** `81acf94`为批准的不可变seed缓存，`9de539a`为容量profile，`5707921`为完整events/versions批量SQL，`769e26f`为组内保守source fence；每包均已push并核验dev远端。正式目录Git更新后安装的wheel SHA-256=`54a7f6c4e60691ceb0ec24c8d7b99c3eacb75c3528e5db91dc0a2bb3b2475a3c`，17个runtime文件与checkout一致，两个服务用户的隔离导入通过。备份/部署证据：`/var/lib/tradecore-deployments/20261021-s4q-769e26f`。性能候选仅在受控资格实例启用，正常服务不自动采用；FULL、DELETE、256/512、1,024 seed上限、原队列及原CPU/内存均保持。

**正确性。** Linux 224 passed、2个显式规模skip（84.64秒），另行100K deferred八表/回放差分1 passed（195.86秒）。覆盖实际kill、FULL后/缓存发布前终止、错误hash/范围/部分batch、orders/facts/events/versions/scope flush错误整组回滚、缓存命中后非法可变状态拒绝、跨scope/namespace/owner拒绝。记录位于`20261021-s4q-validation`及生产evidence中的对应JUnit。规模进程退出后systemd输出的2.1MiB不是有效峰值，不能用于资源结论。

**最新隔离容量，非UDS吞吐证书。** 三轮CPU65、MemoryMax=6GiB、GC开启且700/10/10、100K订单/400K facts/200K events、源准备在计时前完成，采用同一生产grouper和FULL SQL。数据根`s4q-capacity-cache-256-100k-r1`至`r3`，资源采样归档`20261021-s4q-capacity-validation`。

| 轮次 | 计时秒 | fact/s | event/s | 完整1秒桶最低facts | live cgroup peak bytes |
|---|---:|---:|---:|---:|---:|
| R1 | 44.031068 | 9,084.49 | 4,542.25 | 8,704 | 3,807,543,296 |
| R2 | 44.129994 | 9,064.13 | 4,532.06 | 8,704 | 3,809,812,480 |
| R3 | 43.699745 | 9,153.37 | 4,576.69 | 8,704 | 3,791,794,176 |

每轮所有完整1秒桶≥8K，memory.events的high/max/oom/oom_kill均为0。缓存均1,024单/2,105,344编码bytes；paired源同组更新因此cross-tx hit=0，不能据此声称真实延迟收益。没有GC关闭、等批、删事实或改FULL。

**最新真实公共UDS，30秒共同窗口，每类计划60,000。** 服务器64–71及SMT160–167，发生器72–75及SMT168–171，事件观察76/77及SMT172/173；projector仅65。使用同一逐请求trace和commit-only探针配置，不开启S3连接复用。根`s4q-uds-plain-r1`至`r3`；每轮新namespace，不覆盖原失败结果。

| 轮次 | Submit offered/成功/窗内 | 热页offered/成功/窗内 | 投影lag P99上下界，秒 | final source→投影结束，秒 | 结论 |
|---|---|---|---|---:|---|
| R1 | 60,000/60,000/60,000 | 60,000/60,000/60,000 | 0.581960–0.631767 | 0.547992 | 本轮投影lag通过，端到端SLO失败 |
| R2 | 60,000/60,000/59,987 | 60,000/60,000/59,997 | 0.179569–0.230949 | 0.122273 | 本轮投影lag通过，窗口/端到端SLO失败 |
| R3 | 45,875/45,874/45,866 | 45,875/45,875/45,872 | 已产生部分0.057737–0.104137 | 已产生部分0.033043 | 冷准入拒绝导致降档，整轮失败，不能算第三轮2K证书 |

R1/R2均120,800源batch完整配对，241,600 facts/120,800唯一FULL事件与60,400 Mock订单闭合，source join通过。Submit scheduled P95/P99分别46.432/61.735及39.518/52.593ms；热页分别40.042/55.017及33.473/46.203ms，仍未达到原端到端门槛。消费者source→FULL checkpoint P99分别10.301/8.967秒，不能与约0.6/0.23秒投影lag混为同一指标。每轮最多1,024缓存单/2,234,368编码bytes；R1/R2累计hit分别26,764/22,701、miss均60,400。

R3一笔返回`RATE_LIMITED`，trace明确`failure_stage=cold_admission`、`cold_lookup_attempts=3`；该发生器分片停止后续发放，其他分片继续，合计28,250计划未发。最终`cold_admissions_rejected=1`、`overloaded_connections=0`、`lane_rejected_connections=0`，不是UDS接入容量耗尽。当前冷索引`validate`要求lookup miss证明与最新**全局generation**相同，其他订单归档也可能使它过期；三次均过期后正确fail-closed，但目标压力下可用性不达标。未把重试次数调大、未忽略generation或放行未知absence。仅已产生的185,096 facts/92,548 events追平并被消费，原计划241,600 drain目标无法达到，事件报告`FINAL_EVENT_DRAIN_TIMEOUT`、完整source join失败；不能绕过该失败或把已处理子集升级为全计划证书。服务unit的Result=success只说明受控收尾成功，不代表性能验收成功。

S3连接复用单轮对照`s4q-uds-readreuse-r1`未改善端到端结果（Submit/热页scheduled P95约51.741/45.449ms），本次不纳入选型；单轮也不证明普遍退化。

**HOT对照，不能宣称绝对门槛通过。** 每轮1,000预热＋10,000单inflight样本，三组开/关S4候选，双方相同逐请求trace及commit-only探针。源均44,000 facts/22,000 batch、11,000订单闭合，trace零丢失且完整关闭；与旧full逐SQL探针结果不混池，也不当作S0完整采集开销资格。根`s4q-hot-opt-off/on-r1..3`，汇总在部署目录`hot-ab-summary.json`。

| 配对 | P95 off/on，ms | P99 off/on，ms |
|---|---|---|
| 1 | 0.429836 / 0.435705 | 1.400435 / 1.540275 |
| 2 | 0.429686 / 0.433101 | 1.658323 / 1.493695 |
| 3 | 0.429916 / 0.438228 | 1.388687 / 1.717812 |

全部10,000请求成功；P95增幅0.8%–1.9%，但P99有双向波动，不能声称全部分位数无倒退；六轮均未满足HOT绝对P95≤0.30ms/P99≤0.50ms。因此S4整体仍为`IMPLEMENTED_NOT_ACCEPTED`，不解除S8/十二小时/READY门禁。当前还需冷准入证明失效逻辑的单独设计及竞态验收，以及入口端到端/S6消费闭环优化；忽略全局代校验、增加重试/容量或重跑挑选通过轮次都不是已批准方案。

收尾核验：正常TradeCore、fault-supervisor、Consumer UDS三个服务均active/running、Result=success、NRestarts=0，qualification inactive/success；未重启主机、未修改其他项目/global IRQ，未启用EMT/QMT或交易READY。

### 2026-10-22 冷准入失效范围修复：设计与逐步验收

用户继续授权处理上一轮阻塞。实现及Linux正确性子包已通过，实际组合性能仍未通过，状态`IMPLEMENTED_NOT_ACCEPTED`，不将局部修复提前记为S4通过。

1. **R0合同。** 原Bloom只是lookup提示；原SQL DELETE/FULL、append-only exact row、archive FULL后发布/receipt后才能退热、账户锁内binding/hot tail/幂等检查全部保留。默认关闭`scoped_cold_lookup_epochs`，只在显式候选中测试，旧模式按原全局代规则运行。UDS请求/响应及retryable错误合同不变，不增加三次重试或任何队列/连接/数据库上限。
2. **R1证明范围。** 固定65,536个uint64失效版本（512KiB，包含在原6GiB上限内），完整`account/strategy/order_ref`哈希确定分片。不是每订单字典，不随历史增长。每次成功FULL归档在同一原publication锁内提升相关分片版本，然后发布Bloom/global generation和receipt；同key必在同分片，哈希碰撞只造成保守失效。proof仍携带原global generation，新增内部`lookup_epoch`，HMAC域隔离并绑定Journal、分片算法/容量、完整key、原generation、epoch及payload hash；验证原generation不得来自未来，epoch必须等于当前对应分片版本，MAC必须匹配。不能只改字段使旧proof变有效。重启生成新owner nonce并重建，旧proof不可跨实例复用；SQL schema不变。计数耗尽或FULL后publication未知必须poison，不允许wrap或继续签发absence。
3. **R1负面验收。** 不相关归档连续推进后，当前key的原miss仍有效；同key归档、分片碰撞、字段/MAC/payload篡改、future generation、错误owner/Journal、重启和关闭均应拒绝；强制Bloom false positive仍执行exact SQL。原账户锁内再查hot tail，旧miss不能覆盖并发新单、同key归档/退热或binding改变；重复只返回同一OMS ID，冲突不变成新单。SQL失败不得提前改变epoch；FULL结果未知及publication失败后所有proof拒绝。probe/validate不增加SQL；固定512KiB、原pending/queue不变。
4. **R2 Linux与发布。** 真实AF_UNIX/SO_PEERCRED挂起/恢复竞态、三次失效上限、停止/drain、进程cold owner未知提交/kill、恢复与旧模式兼容全部运行；模块通过后push并核验远端。正式目录仅Git拉取，构建/安装wheel并核对变更文件、双服务用户导入和CPU分区，保留回滚wheel，默认不开新模式。
5. **R3实际验收。** 冻结原CPU/MemoryMax/256 batch/512 fact/1,024 seed/FULL/队列，显式开新模式，三轮独立namespace各30秒2K提交＋2K热页，并发事件消费仍走公共UDS，保留逐请求/源→投影FULL/消费者checkpoint完整配对。每类60,000计划与offered/成功/窗内/late/未发守恒，冷准入拒绝必须为0；任何失败整轮保留，不补发、不重跑挑选通过。然后同口径HOT对照，确认新增固定epoch不使低压明显倒退。仍需分别报告端到端SLO、投影lag≤1秒和完整消费延迟，不能因修复冷拒绝而自动通过S4/S8/十二小时/READY。

#### 冷准入实现与正式安装：`414cc42`

固定epoch、域隔离HMAC、publication未知poison及旧模式兼容已实现，默认仍关闭。Linux正确性回归201 passed、1显式skip，日志`/var/lib/tradecore-deployments/20261022-s4r-r2-validation/linux-core.log`。首次执行误只分配1个物理核，不满足原8核亲和性用例，失败保留在`20261022-s4r-validation`；更正为原项目专用核后完整重跑，未降低用例要求。正式目录Git拉取、wheel安装、20项源码hash一致及双用户隔离导入通过；安装清单`20261022-s4r-414cc42/deployment.json`，wheel SHA-256=`093f00b6ccd03b5d3e6ff4504000ac7255b459d548fc7542be644dd88187bf65`。正常服务不开新候选。

以下均是各自独立namespace的30秒2K＋2K候选/对照，不拼成同配置三轮合格证书；每类计划60,000，客户端scheduled分位数包含发生器等待。

| 数据根（均位于`/var/lib/tradecore-longrun-qualification/`） | Submit/热页成功 | 合计未发 | Submit/热页scheduled P95，ms | 结论 |
|---|---:|---:|---:|---|
| `s4r-uds-r1`，scoped证明、原reactor | 56,174 / 56,174 | 7,652 | 279.446 / 277.610 | 未全量发出，FAIL |
| `s4r-control-global-r1`，同payload旧全局证明 | 60,000 / 60,000 | 0 | 201.327 / 200.147 | 窗内59,703 / 59,711，SLO FAIL |
| `s4r-fixed-uds-r1`，scoped证明＋已有固定reactor预算 | 51,994 / 51,993 | 16,013 | 284.951 / 278.659 | 未全量发出，FAIL，不采用该预算候选 |

三轮cold拒绝、入口overload、lane拒绝均为0；前两轮scoped证明失效0，旧模式失效27但重查成功；固定预算轮scoped失效0。scoped固定内存524,288 bytes，队列及外层6GiB未变。未发轮最终固定241,600事实目标无法达到，consumer drain/source join失败，不可将实际产生的子集改当全计划通过。旧模式对照241,600 facts/120,800唯一FULL事件闭合，完整source→消费checkpoint P99约25.132秒，也不是性能通过。

逐请求以完整request-id SHA-256配对客户端raw与服务端trace，客户端raw manifest/hash chain已验证，结果分别存各数据根`request-pair-diagnostic.json`，不把不同来源的总体分位数相减。scoped/旧全局/固定预算分别配对112,348 / 120,000 / 103,987请求；客户端send完成→服务端首次读取P95分别146.717 / 138.228 / 137.447ms。scoped Submit的schema验证后→handler完成P95约0.243ms。signed时间差保留；send早于accept是有效排队证据，不取零、不删除。瓶颈主要在已发送首帧等待服务端处理，而不是客户端connect→send；尚不能仅凭单次对照判定冷证明模式无额外开销。

独立单核proof微诊断（不是UDS证书）同样100K次probe＋validate：旧模式约5.81–5.89µs/对，新模式7.30–7.38µs/对。不能把约1.5µs差值解释为140ms等待的全部原因。附加`s4r-stack-uds-r1`为99Hz非阻塞调用栈诊断轮，采样扰动使78,177计划未发，明确不作验收；原始`gil-stacks.txt`与`thread-scheduling.jsonl`保留。前9个负载秒main线程CPU约0.73–0.76秒/秒，OS runqueue等待约0.5–0.6ms/秒；同进程Mock生命周期亦占用GIL。该证据支持检查进程内计算/争用，不支持宣称服务器物理核不足或擅自更改全局IRQ。旧三轮GC开启700/10/10，最大暂停23.6–35.2ms，不能单独解释持续首帧等待。

### 2026-10-22 S4S：冷归档复用本次原恢复已验证的admission

冷归档原本先完整恢复checkpoint，再单独decode同一admission以提取account/strategy/order_ref。仅改为私有单订单no-write verifier捕获原`_decode_recovery_admission`的返回值，且**原restore成功后**才派生key；每次调用仍完整执行canonical seed、可变state/event/reservation和Journal/scope校验，失败不能返回identity。临时对象不跨订单、调用、事务、进程或重启，不新增缓存/队列，不改FULL或epoch/MAC。先前正确性证明仍需在最新代码重跑。

验收：同一checkpoint连续三次各执行一次原canonical decoder，不得跨调用命中；state/revision/fill/trade/seed/event identity/source/缺终态/错误Journal与原restore同样拒绝，active不能退热；原冷热归档/并发/进程丢回复与未知FULL矩阵仍须通过。模块本地52 passed、2显式skip，Ruff/Pyright通过。Linux、正式部署和新的公共UDS组合性能待执行；不预标S4通过。

#### S4S最新验证与部署：`0b3284d`

Linux完整相关组合214 passed、1显式skip（59.64秒），包括原冷归档、scoped/旧模式真实UDS竞态、checkpoint进程、恢复及投影组合；日志`/var/lib/tradecore-deployments/20261022-s4s-validation/linux-core.log`。正式目录Git拉取后安装wheel，SHA-256=`0867081baac0218521f6ec1134cf88501f20a59d6522d30d1d6309c866b340c0`，20项安装源码hash一致、双服务用户隔离导入和CPU分区通过。安装及回滚包位于`/var/lib/tradecore-deployments/20261022-s4s-0b3284d/`。

最新代码按原8物理核服务器＋隔离发生器/事件消费者、6GiB、GC开启700/10/10、256 batch/512 fact、1,024 seed、FULL、原队列连续三轮30秒2K＋2K。均开scoped证明；没有采样器，没有同核或切换间隔实验参数。三轮仍失败，不能算三轮通过。

| 数据根 | Submit成功/窗内 | 热页成功/窗内 | 合计未发 | Submit scheduled P95/P99，ms | 热页scheduled P95/P99，ms |
|---|---:|---:|---:|---:|---:|
| `s4s-uds-r1` | 45,919 / 45,919 | 45,917 / 45,917 | 28,164 | 341.049 / 367.264 | 339.739 / 364.642 |
| `s4s-uds-r2` | 37,962 / 37,962 | 37,959 / 37,959 | 44,079 | 349.176 / 379.847 | 348.127 / 377.225 |
| `s4s-uds-r3` | 52,203 / 52,203 | 52,203 / 52,203 | 15,594 | 332.661 / 352.059 | 331.088 / 349.700 |

每轮各类计划60,000。发生器有界release队列达到上限后停止对应分片，未补发；成功率不能以已发送子集冒充全计划。三轮server cold拒绝、scoped proof失效、入口overload、lane拒绝均为0；cold owner未failed，主机未重启，未调用Adapter。进程存活期间采集的cgroup内存峰值分别2,906,083,328 / 2,694,434,816 / 3,354,959,872 bytes，`memory.events` high/max/oom/oom_kill均0，不是达到6GiB上限或OOM导致。源commits原始数据/关闭manifest/hash验证通过，实际batch/facts为92,638/185,276、76,724/153,448、105,206/210,412；完整计划241,600目标及source→consumer join均未通过。实际子集的完整性不是全计划完成证书。

三轮客户端raw/服务端逐请求trace分别完整配对91,836 / 75,921 / 104,406条，实际send→首读P95为139.077 / 142.903 / 132.944ms。客户端call→connect P95仅0.627 / 0.770 / 0.699ms；首读→dispatch P95为9.515 / 12.231 / 10.008ms。证明首帧等待仍是显著瓶颈，不能把失败全部归因于发生器，也不能以扩大客户端队列掩盖。服务端完整request trace关闭/校验分别覆盖93,711 / 77,747 / 106,372请求（含准备期及事件消费请求），与仅load原始配对条数不同。

**不采用的调度对照，单列保留。** `s4s-samecore-uds-r1`只去掉已有background CPU选项，使父进程线程按已有默认共用reactor核；各类计划仍60,000，但Submit/热页只完成8,952/8,951，102,097未发，明显不满足目标，不采用。`s4s-gil500us-uds-r1`仅在隔离候选bootstrap将该Python进程switch interval改为0.0005秒，启动日志实读值已归档，不改主机/其他进程：各类51,402成功，17,196未发，Submit/热页scheduled P95为340.001/338.428ms，配对send→首读P95仍140.397ms，也不采用。正常服务没有新CLI开关或参数变更；CPU总额、队列、FULL均未调整。上述两轮不能混入原配置三轮，也不能据此宣称一般同核布局/GIL间隔必然更好或更差。

**scoped证明HOT开/关对照。** 同一`0b3284d`及S4投影配置、原分核布局、每轮1,000预热＋10,000单inflight真实公共UDS，交替off/on顺序，根`s4s-hot-epoch-off/on-r1..3`。每轮10,000成功、唯一LSN完整，另1,000预热成功，request trace11,001条关闭且零丢失；源44,000 facts/22,000 batch、11,000冷归档订单，commit-only stage完整源前缀均验证通过。旧全局proof失效分别10/8/7，scoped为0，但均成功重查，不能据此伪造性能通过。

| 配对 | P95 off/on，ms | P99 off/on，ms |
|---|---|---|
| 1 | 0.498940 / 0.498750 | 2.173488 / 2.200298 |
| 2 | 0.523477 / 0.500382 | 2.386288 / 2.131574 |
| 3 | 0.497639 / 0.524989 | 1.923352 / 2.163563 |

所有HOT绝对P95≤0.30ms/P99≤0.50ms仍失败，开关差异双向波动，第三对P95约+5.50%、P99约+12.49%，不能宣布低压完全无回退或自动启用scoped模式。汇总证据`/var/lib/tradecore-deployments/20261022-s4s-0b3284d/acceptance-summary.json`，逐轮原始文件保留。

**当前状态与下一范围。** 冷准入证明/恢复校验消冗余的实现和正确性子包通过，性能组合仍`IMPLEMENTED_NOT_ACCEPTED`，S4/S8/十二小时/READY不解除。已经排除了本轮OOM、服务端容量拒绝及“全部由新proof计算耗时引起”的解释；对照不支持继续仅调reactor预算、线程同核或GIL间隔来收口。下一工作需转入S1提交主路径的计算减负/执行隔离设计，并单独证明当前binding/risk检查、账户线性化、幂等、Journal-before-receipt、未知结果fail-closed和有界背压不变，不得以跳过校验、无限队列或放宽SLO替代。

收尾正常TradeCore、fault-supervisor、Consumer UDS均active/running、Result=success、NRestarts=0；qualification inactive/success。临时owned drop-in清除，正常服务未开scoped proof/实验GIL参数；全部失败证据、Journal与回滚wheel保留。service Result=success只表示受控收尾成功，不表示性能通过。

### 2026-10-22 提交主路径减负与执行隔离实施合同

用户明确授权开始改造。沿用原资源、FULL、原入口/冷准入上限、256 batch/512 fact及1,024 seed缓存边界；保持纯Python，不增加同账户并行写者，不调用Adapter、不重启主机、不修改其他项目。消费者仍只调用原Consumer V2公共UDS；不能以内部成功代替客户端完成，不改变未知结果的安全语义。阶段提交到当前dev并核验远端，候选性能通过前不解除S4/S8/十二小时/READY。

1. **E1 同次准入计算减负。** Base/Retained共用私有continuation，公开admit各自完整校验确切不可变binding/risk/decision/reservation输入；只在同一次调用中进入continuation，不签发可跨调用复用的授权。intent hash每次准入只算一次，在账户锁外完成；mutable dedup/容量/风险占用仍在原账户锁内，事实提交成功后才安装hold/order/event。冷恢复重试是新的admit，必须重新检查当前输入。补齐冷命中的负时间、reservation symbol一致性检查，不允许冷幂等路径弱于普通准入。验收：canonical reference与Retained的accept/idem/conflict/terminal事实逐字节一致；校验/hash次数、锁外hash、过期/撤销/错scope与无事实副作用、Journal失败无hold/热态安装；原恢复/投影回归通过。性能效果必须另测，不凭减少函数调用宣称SLO通过。
2. **E2 有界单owner提交调度隔离。** 增加默认关闭的候选提交执行通道，所有Submit及冷lookup恢复仍由同一个执行owner串行完成；socket读写、首帧、超时、结果发送由reactor拥有。worker不触碰客户端socket。排队/运行/未领取结果都计入通道限额，且从原decoded-ready总预算中扣除，不增加总容量；满时给明确背压。保留原三次冷失效规则，lookup只查证，不在Future回调中准入。已开始执行后断连不能被当成回滚、重试或“确定没提交”；未开始执行的断连/停止应保守拒绝。close/drain异常必须失败，不将悬挂worker写为success。验收：慢Submit不阻塞独立只读请求/首帧；同key同OMS ID、每次Journal-first、冷命中与全量负面矩阵、half-close/真实close/停止、partial frame/response、绑定变化、结果配对/trace完整、credit上限、故障无自动补发。线程阶段只隔离调度，仍共享GIL，不得称为进程隔离或数量级性能验收完成。
3. **E3 真正进程隔离的发布前提。** 将E2端口实现替换为spawn的唯一OMS owner前，必须把Journal/OrderBook/Mock生命周期/冷退热的归属一起迁移；禁止fork继承活跃锁/线程/写者lease或在两个进程保留可变Book。IPC用有界明确schema/请求关联/启动generation，禁止凭超时自动重放未知写；binding撤销、风险时效、owner断开/kill、FULL后丢回复、恢复证明和关闭顺序需有专项验收。前端读模型仍只能由已提交事实驱动，不能读取另一个进程的私有Book。独占CPU来自原项目资源，不扩大外层MemoryMax/队列。E3未实现前明确保持该阶段未通过。
4. **E4 同口径性能与发布。** E1/E2完整功能包结束后统一执行Linux故障/恢复回归与真实公共UDS HOT/LOW/30秒2K＋2K混合三轮；每类60,000计划、offered/成功/窗内/未发守恒，保存首读→执行入队→owner开始→Journal commit→owner结果→发送的逐请求时间。低压仍按绝对HOT P95≤0.30ms/P99≤0.50ms及开/关回退检查；高压保留原端到端、generator queue、投影≤1秒与完整消费检查。每个调度/隔离profile独立归档，不混池，不重跑挑选、不扩大队列，不把线程隔离测成进程隔离。正式目录Git部署、安装wheel及源码hash/双用户导入/正常服务健康验证保持原要求。

E1已实现，本地81 passed、1显式skip，Ruff/Pyright通过并push `48d26c9`；Linux尚待执行。E2a已实现独立有界single-owner执行原语与原ready队列reservation credit：queued/running/uncollected结果均占credit，停止/观察到的取消/过期只阻止未开始任务，已执行错误保留unknown，不重试/不返回虚构negative receipt；冷需求只返回key/port，不能把异常栈或准入搬到Future callback。ready reserve/pop-excluded保留原总/分lane上限及FIFO；暂不接入正式服务。模块本地22 passed、Ruff/Pyright通过；新增通知故障用例首次误假设Windows socketpair关闭对端后紧接的send必失败，已改成确定性syscall故障注入并完整重跑，未降低通知故障必须fail的标准。真实Linux/服务接线和完整E2/E3/E4仍未通过，不宣称吞吐改善。

E1/E2a的Linux组合245 passed、1显式skip（56.91秒），日志`/var/lib/tradecore-deployments/20261022-submit-e1-e2a-validation/linux-core.log`。E2b接线已实现，候选开关`--isolate-submit-execution`默认关闭，状态明确`THREAD_SINGLE_OWNER`或`INLINE_REACTOR`；新worker来自原background CPU集，不增加CPU/MemoryMax。Submit/Cancel及冷重查恢复均经同一owner，读/写客户端socket仍在reactor；原ready credit移交执行slot，满owner时不取走WRITE FIFO，仍可调度read；排队/运行/未领取结果合计最多128且总decoded credit不超过原pending上限。Future只通知，恢复任务仍重新走当前gateway/binding/risk检查。

E2b覆盖观察到的full-close取消排队任务、SHUT_WR仍可收回执、停止只阻止未开始任务、相同key唯一事实/hold、FIFO与partial frame/response、默认关闭及真实后台核亲和性。执行错误可能已提交时poison整个owner、禁止后续新写/自动重试、不伪造retryable negative；owner关闭未知/超时则保持未验证drain，Candidate不得提前关闭仍被引用的Journal/Book或发布正常managed exit。只有明确未开始执行而仍可响应时可返回`RATE_LIMITED/SUBMIT_NOT_STARTED`。新增有界最多4次attempt时间明细（enqueue/owner observed/finished/started）及reactor结果观察时间，保留冷重试原始阶段，不把线程队列等待漏出总延时。

E2b本地32 passed、100项真实Linux用例显式skip，Ruff/Pyright通过；服务接线的Linux功能/故障验收与服务器HOT/混合性能尚待执行，因此不能视为E2/E4通过。E3进程隔离未实现，当前线程owner不得冒充已解除GIL瓶颈。

E2b随后在Linux完整组合300 passed、2显式skip（75.75秒），日志`/var/lib/tradecore-deployments/20261022-submit-e2b-validation/linux-core.log`。追加保护：仅在listener调用handler**之前**的schema/WRITE类别检查失败才标为known precheck，关闭该连接但不poison owner；其余执行/存储异常仍视为unknown并封锁后续写，绝不通过异常类型猜测“没提交”。schema仍只验证一次，不增加跨请求schema/授权缓存。补充无效schema后正常请求继续成功、64-byte预算下分段帧/SHUT_WR/分段回执测试；单元本地23 passed、7项Linux显式skip、Ruff/Pyright通过，追加Linux回归待执行。该实现仍仅是线程执行通道，不是E3或性能验收通过。

#### E1/E2功能验收及性能否决：`e0d942f`

追加后的Linux组合303 passed、2显式skip（76.04秒），包括真实peer/schema失败后的后续正常请求和分段WRITE回执，日志`/var/lib/tradecore-deployments/20261022-submit-e2c-validation/linux-core.log`。本次变更模块Ruff/Pyright为0错误；全库Pyright扫描另发现50项错误，集中在本次未改动的EMT Adapter文件，本轮`66b25b7..e0d942f`无Adapter差异。没有顺手修改这些接口，也不宣称全库静态检查通过。

正式目录Git拉取并安装wheel，23项安装源码hash与checkout一致、双用户隔离导入及CPU分区通过。部署目录`/var/lib/tradecore-deployments/20261022-submit-e0d942f/`，wheel SHA-256=`05b16d05ec600539ea489288657be860a8b61c70c44e5081edc81eb369e97b2b`。正常服务仍不开线程隔离开关；候选实验保持原8物理核、6GiB、256/512、1,024 seed、原队列及FULL，双方均开scoped cold proof和commit-only证据。

一组同版本30秒2K＋2K开/关对照（不是三轮合格证书）：

| 数据根 | mode | Submit offered/成功/窗内 | 热页offered/成功/窗内 | 合计未发 | Submit scheduled P95/P99，ms | 热页scheduled P95/P99，ms |
|---|---|---:|---:|---:|---:|---:|
| `submit-inline-uds-r1` | INLINE_REACTOR | 60,000 / 60,000 / 59,788 | 60,000 / 60,000 / 59,800 | 0 | 94.110 / 112.853 | 87.556 / 106.299 |
| `submit-thread-uds-r1` | THREAD_SINGLE_OWNER | 13,346 / 13,346 / 13,346 | 13,345 / 13,345 / 13,345 | 93,309 | 372.769 / 383.779 | 369.885 / 380.895 |

关闭时全计划发出且最终成功，但窗内/SLO仍失败；source→consumer完整120,800事件、241,600 facts/60,400订单闭合，消费P99约20.720秒，仍不满足长期门禁。开启时发生器release容量达到水位停止分片，不能以13K成功子集宣称2K持续容量通过；原241,600最终目标与source join失败。开启/关闭都没有入口overload、lane拒绝、cold拒绝或proof失效，没有OOM；live cgroup峰值分别3,570,380,800 / 1,477,832,704 bytes，memory.events high/max/oom/oom_kill全0。开启时每秒采样最大submit pending为3（仅采样值，不冒充真实瞬时最大值），不是有证据显示128执行slot耗尽。

原始client/服务端trace/source commits的关闭manifest、hash与时钟身份均已校验。关闭时配对120,000条load请求/60,000条提交；开启配对26,691条load请求/13,346条提交，后者每条确认为1次owner执行，未漏计中间重试。逐笔诊断位于各根`submit-pair-diagnostic.json`，仅已发子集，不升级为全部计划证书；分位数独立计算，不能直接相加。

| 实际配对段，P95 ms | INLINE_REACTOR | THREAD_SINGLE_OWNER |
|---|---:|---:|
| 客户端send完成→服务端首读（Submit） | 84.461 | 146.885 |
| 首读→reactor dispatch（Submit） | 15.893 | 10.357 |
| 提交执行排队 | 不适用 | 2.630 |
| dispatch/owner开始→源commit前 | 0.220 | 0.331 |
| 源mmap commit interval | 0.009044 | 0.008333 |
| 源commit后→owner结果 | 不适用 | 0.037 |
| owner结果→reactor领取 | 不适用 | 4.489 |
| reactor领取→send完成 | 不适用 | 0.162 |

源mmap commit interval不是物理SSD FULL耗时；SQL cold/projection FULL要求未改变。owner开始→源commit前包括实际校验/计算/锁与调度，不把墙钟直接冒充CPU。线程通道未改善首读排队，反而增加任务入队/唤醒/结果回收交接；它仍共享GIL，不能依据功能通过将其作为性能选型。

三组HOT开/关对照，同版本每轮1,000预热＋10,000单inflight、完整公共UDS，原CPU分布与相同证据开销，根`submit-hot-thread-off/on-r1..3`：

| 配对 | P95 off/on，ms | P99 off/on，ms |
|---|---|---|
| 1 | 0.474025 / 0.638363 | 2.088130 / 2.679924 |
| 2 | 0.481206 / 0.625023 | 1.924353 / 2.754866 |
| 3 | 0.489067 / 0.601817 | 2.192196 / 3.685578 |

每轮10,000成功且源44,000 facts/22,000 batch完整投影stage校验通过，但开启P95三对均明显退化，双方也未满足HOT绝对标准。故E1计算重构功能通过；E2仅功能/故障通过，线程候选**性能否决、默认关闭、不推广**。未用旧版本不同时间段数据推算精确E1收益。S4/E4仍未通过，E3仍未实现，不启动十二小时或READY。汇总`/var/lib/tradecore-deployments/20261022-submit-e0d942f/acceptance-summary.json`。

#### E3后续代码包：唯一OMS进程owner，不沿用共享GIL闭包

1. 先定义明确有界业务IPC schema：启动generation、serial、当前授权scope/epoch、原始业务幂等key、deadline、完整请求和关联回复。队列credit继续来自原decoded-ready预算，排队/执行/未交付合计守恒，不叠加另一张无限任务表。E2的Python闭包和resolver对象不允许直接pickle成进程协议。验收包含错generation/serial/scope、超限/截断/乱序、不可关联回复及断链，均不能形成成功回执。
2. spawned owner独占完整可变OrderBook、Journal writer lease、Mock生命周期和冷退热协调；不能只复制admission方法，也不能在前后两个进程各保留一张可写Book。projection/cold SQL/segment/read子进程保持原角色，frontend只持有已提交读模型/受控只读能力，不能取得另一个Book的私有引用。启动恢复完成且owner身份/源prefix验证后才能打开公共入口；所有子进程仍归同一个原6GiB cgroup和原专用核集合。
3. 前端拥有公共SO_PEERCRED、帧、socket写回和deadline；owner执行时重新确认当前绑定、风险与账户线性化。若使用私有内部UDS或持久IPC连接，必须验证私有目录/连接owner/启动generation、原客户principal归属及回执关联；持久连接不能把旧session epoch当永久授权。内部read服务与write单owner可分别调度，但公开API、cursor及事实来源不改变。
4. 取消只接受owner确认的NOT_STARTED作为确定无提交证据；STARTED/COMMITTED/断链丢回复均禁止自动重发。owner crash/kill/失联使前端停止新写，不能自行创建新owner继续交易；恢复仍通过原source/cold覆盖证明再发布DISARMED。before append、commit word后/回执前、FULL后丢回复、恢复重复key、旧generation回复和关闭顺序均须逐项故障验证。
5. 完整功能结束后再统一验收：同代码布局对照HOT/LOW与三轮2K＋2K，每类60,000全发/成功/窗内守恒，记录前端接纳、IPC入队/owner领取、源commit、IPC结果及客户端完成全链路；包含完整消费闭环与投影lag门槛。先验证真实分进程及唯一writer，再评估性能，不因“不同PID”或只缩短首读就宣称总延时通过。

本次收尾正常TradeCore、fault-supervisor、Consumer UDS均active/running、Result=success、NRestarts=0，qualification已停止，临时owned drop-in已移除；未重启主机或触发EMT/QMT。功能完成与性能否决分别标记，失败证据及回滚wheel全部保留。

#### 2026-10-23 E3a：有界进程协议与原容量守恒

状态：**E3a本地及Linux功能通过；E3整体未完成，未接入生产或资格实例。** 代码为`consumer/owner_ipc_v1.py`，不创建OrderBook、Journal或另一份恢复状态，不更改当前服务的执行路径。不得将协议测试peer或其构造的示例source字段当作真实OMS提交证据。

**消息合同。** 仅有request/cancel/ready/event四种带类型的不可变MessagePack消息，未知字段拒绝。每笔绑定启动generation、单调uint64 serial、consumer/account/strategy、scope SHA-256、authorization epoch、原request ID及业务幂等key、原始帧SHA-256和deadline；字节原样传递，协议层不取代owner的业务schema、绑定、风险和账户锁内校验。request/response最大沿用公共XTC2帧上限，内部元数据另限8KiB。`TCO1`长度/CRC封装拒绝截断、超长、粘连和损坏；CRC不是身份认证，**只能用于可信spawn继承的私有socketpair**，不能放到公共网络。无Python业务闭包、resolver或OrderBook pickle；没有凭证专用字段，也不允许绕过普通数据面schema传入凭证。

**启动与源关联。** 只有匹配启动generation、预期spawn PID和scope的单次ready握手，才允许reserve。该ready是内部启动消息，绝不等于交易`READY`。消息携带恢复Journal ID/LSN/prefix摘要，后续completion要求同Journal、LSN不退回、同LSN摘要不改变。协议只能核对关联与单调性，不能自行证明FULL或源前缀正确；下一包必须由唯一runtime owner验证真实恢复和source，再构造这些字段，公共入口不得在该验证前开放。

**容量与顺序。** `OwnerCreditLedgerV1`只是最多128条身份/阶段记录，不储存另一份请求帧，不新增独立任务容量。每条reserve必须取得原`BoundedFairOperationQueueV2`的WRITE credit；QUEUED、STARTED和完成待交付共同占额，只有确认terminal且客户端交付/处置后才release。单reactor线程持有ledger，串行owner只能启动队首，不能同时有两个STARTED。错generation/serial/key/hash/scope/epoch、重复或乱序回执、错误消息方向、源退回均poison并封锁新写；不能用清空表抹掉failed状态。

**取消和未知结果。** 本地cancel只是请求，不是取消成功；只有owner明确NOT_STARTED并给出CANCELLED/DEADLINE/STOPPED/PRECHECK才有确定未开始证据。开始与取消竞态以owner状态为准。STARTED后丢回执、部分发送超时、EOF或UNKNOWN都不能转成retryable拒绝，必须由调用方立即fence，保留占额直到失败处置；禁止自动重连重放或替换owner。socket收发有最多60秒绝对deadline，无内部重试。交付前还必须提供当前授权，与原请求完整authority一致；旧epoch完成回执也不可交付给已撤销主体。

**已执行本地验收。** 新协议、原线程lane/fair queue和admission回归合计105 passed、1个Linux真实AF_UNIX发送缓冲饱和显式skip；新增测试含真正`spawn`、不同PID、继承socketpair每7字节分段、开始后断链、部分发送故障、超限/CRC/未知字段、授权撤销、同源摘要冲突、取消竞态和额度守恒。Windows TCP-backed socketpair不能假定peer close后首次send立即报错，故本地确定发送故障显式注入；Linux另保留真实AF_UNIX缓冲写满超时用例，不以模拟故障代替该Linux用例。变更模块Ruff和Pyright为0错误。

**Linux验收归档。** 代码包`5595dba`已push并核验远端，服务器仅Git快进源码后，在独立有限systemd测试单元中执行新协议、原lane、admission、公共UDS/冷准入/提交及只读reactor、identity和ColdAware恢复回归：251 passed、2显式skip、189 subtests passed，34.67秒。两项skip均为旧global模式不适用scoped专属用例，不是新IPC故障用例跳过。真实Linux AF_UNIX发送缓冲饱和/部分发送超时、spawn分段和断链均实际执行。证据根`/var/lib/tradecore-deployments/20261023-owner-ipc-e3a-validation/`保留`linux-core.log`、`junit.xml`和`acceptance.json`及文件hash；原专用8物理核集合、6GiB、GC开启，无Adapter调用。它是功能回归，不是吞吐或资源峰值资格证书。未安装新wheel、未启用新路径或重启正式服务；三个正常服务仍active/running、Result=success、NRestarts=0，qualification inactive。正式运行的上一版wheel及其回滚包不变。

**下一包E3b与总体验收。** 仍须把完整Book、writer lease、Mock和冷热退热协调迁入唯一spawn owner，接入前端SO_PEERCRED/当前授权及只读能力、关停/恢复和源commit故障矩阵；不得直接pickle E2闭包或在前端重建可写Book。随后才能部署关闭默认开关的完整候选，并做真实公共UDS HOT/LOW及三轮2K＋2K。E3a通过不代表E3、S4、十二小时或交易READY通过，不宣称延迟改善。

#### 2026-10-23 E3b：完整runtime的真实spawn所有权候选

状态：**E3b唯一owner私有链路功能、故障、CPU布局及原始目录留存验收通过；未切换公共入口，E3整体和性能仍未通过。** `consumer/process_oms_owner.py`新增一次性`SpawnedOmsOwnerCandidate`：父进程只持有有界配置字节、控制socket和明确的Process句柄，不持有Book、writer lease、恢复对象、Mock、SQL owner或游标密钥。spawn子进程才创建完整原`LongRunningConsumerCandidate`，独占Book/Journal/identity/Mock/退热协调，并管理原projector/cold SQL/segment/read子角色。没有复制Book、fork活跃运行时或序列化业务闭包；CPU、GC、FULL与队列规则沿用原组合。配置消息最多16KiB，outer/config/policy未知字段拒绝，拒绝真实Adapter、BENCHMARK、相对路径和使用原公共socket。

**本包边界。** 子进程使用保护目录下0600私有UDS，由原reactor验证本机owner UID/GID并执行原业务链路；原公共socket完全不打开。该私有端点只用于owner资格验证，尚不代表已转发原始上游principal/epoch，也不允许直接叠一层公共ready队列。E3c仍须接入公共SO_PEERCRED、原容量credit、每请求身份和当前授权、IPC状态/时间戳及受控只读能力；完成前不得部署为公共新路径或宣称已隔离其全部调度。E3a业务request/event协议仍待E3c接入，本包只复用其ready及新增stop/stopped生命周期消息。

**恢复与关闭。** ColdAware原恢复遍历逐批已验证且成功apply后累计batch fingerprint，绑定固定domain/Journal/profile/恢复LSN，完成原冷热覆盖验证后才发布`recovered_source_sha256`。不额外遍历源、不改变SQL/coverage schema、不在提交热路径新增哈希。原scope绑定构造提取成纯不可变函数，父子核对同一scope；ready核验generation/实际spawn PID/恢复位置和脱敏trace basename，ready仍非交易READY。受控stop只能匹配本代serial=1；EOF、旧代/错误控制、SIGTERM或kill均不当作干净关闭。子进程先停入口并按原顺序drain全部managed组件，父进程还需核对stopped、退出码0和private managed-exit文件hash/Journal/最终LSN才可置`shutdown_verified`。超时仅处置本次明确创建的子进程，失败不得自动重建owner或重发请求。

共享multiprocessing resource tracker仍由未来独立前端CLI持有；本类关闭只证明OMS managed子树，不能借用/关闭pytest或其他宿主的共享tracker，也不签发全owned-descendants lifecycle证书。启动限定Linux项目CPython3.10、实际同一有限memory cgroup和KillMode=mixed；无公共服务默认开关。

**本地验证。** E3b/E3a、ColdAware恢复、现有长运行candidate/identity/CPU角色回归118 passed、19项Linux专属显式skip，Ruff与变更模块Pyright为0错误。已覆盖配置不产生父进程runtime、禁止真实Adapter/公共socket、严格配置schema和不可变生命周期包；新增恢复摘要与真实逐批fingerprint差分及无新增事实时重启稳定性。Linux还须实际执行独占kernel lease、完整Mock委托/终态/快照/事件、手工恢复重复key、owner kill、控制EOF/旧代stop、第二owner拒绝，以及原长运行回归；不能把本地skip算为这些能力已通过。E3/S4/性能/十二小时/READY仍未通过。

首轮Linux组合记录保留于`/var/lib/tradecore-deployments/20261023-oms-owner-e3b-validation/`：245 passed、2 skip、1 failed。唯一writer、kill/控制EOF/旧代stop和第二owner用例通过；失败是组合测试把热态GetOrder终态等同于异步FULL快照已经追平，实际返回合法的旧一致版本0单。测试修正为先经公共CreateReplayPartitions读到完整20个事件的投影fence，再断言10单快照；不读取父进程私有Book、不固定sleep猜测、不改变服务端SLO或放宽快照标准。修正后完整Linux组合仍需重跑，原失败不覆盖。

修正后`15c882e`完整Linux组合246 passed、2显式skip（49.85秒），所有新owner实际spawn、唯一kernel lease、10笔Mock及40 facts/20事件闭合、终态/分页快照、手工恢复重复key不增事实、kill/控制EOF/旧代stop、第二writer拒绝均通过；两个skip仍是旧global模式不适用scoped专属用例，不是owner故障跳过。证据根`/var/lib/tradecore-deployments/20261023-oms-owner-e3b-r2-validation/`含JUnit、日志、acceptance与hash。补充真实owner五个子角色和全部线程CPU亲和性用例：沿原8物理核布局，验证父进程掩码未改变、子主线程reactor核、projector/cold/segment/两个read owner各原专用核、其余线程背景核；该新增用例本地12 passed/6 Linux skip组合通过，Linux追加执行待归档。未装wheel或切换公共服务，未使用Adapter，正常三个服务active/running、success、NRestarts=0。

`b5f1a6c`追加全角色CPU用例后，Linux完整247 passed、2 skip（50.94秒），日志和JUnit位于`20261023-oms-owner-e3b-r3-validation`。上述CPU布局均实际验证。R1–R3的JUnit/日志保留，但owner夹具当时使用TemporaryDirectory，结束时已自动清理临时source，不能声称还能复核其原始Journal。为补强审计，真实owner测试改为强制提供private evidence root，登记并保留每个短路径原始runtime目录（含Journal/恢复状态/私有identity/managed退出文件），成功或预期故障均不自动删除、不复制live mmap作证明；目录700、证据600且只留服务器，不提交密钥。最新夹具需再次Linux完整验证并归档原始源，R1历史失败不被覆盖。

**E3b最终本包归档：`17920aa`。** 保留原始运行目录后，Linux完整247 passed、2个旧模式专属skip（51.09秒）；所有六个新owner功能/故障/CPU场景实际执行，没有owner专属skip。证据根`/var/lib/tradecore-deployments/20261023-oms-owner-e3b-r4-validation/`含JUnit、日志、acceptance及hash；`owner-runtimes/`登记六个原始私有短路径，保留其完整Journal/恢复/trace/managed文件，acceptance逐源文件记录SHA-256。原始目录包含私有identity，只能在服务器受控访问，不作为上游公开包或Git内容。首次失败日志仍保留，不将功能回归中systemd退出后的峰值当作长期资源性能证书。

最终检查三个正常服务均active/running、Result=success、NRestarts=0，qualification inactive；未重启主机、未装新wheel、未启用公共新路径或EMT/QMT。下一包为E3c公共前端与业务IPC接入：必须保留原始客户principal和当前授权、共享原逻辑容量、完整请求/回复与未知结果状态、只读交付fence及关闭顺序，再做commit前后/丢回复恢复故障矩阵和HOT/LOW、三轮2K＋2K。当前不宣称提交延迟改善、S4通过、十二小时或交易READY。

#### 2026-10-23 E3c1：继承WRITE业务通道及唯一报单入口

状态：**E3c1继承WRITE业务通道本地及Linux功能/本包故障验收通过；完整E3c未完成。** 本包不是公共前端，也不是吞吐证书。不能在E3b私有UDS前再叠加ready队列，或把私有socket的UID当作原始消费者身份。

1. **原身份与路由。** `owner_submission_ipc.py`通过新继承socketpair传递不可变OwnerRequest，身份关联包含原始SO_PEERCRED的PID/UID/GID、consumer/account/strategy、scope/epoch、request ID、原业务key、原始XTC2帧及SHA-256。owner核对启动generation和递增serial，再核对原始peer是否在配置allowlist、帧operation/request ID/account/key/strategy与信封完全一致；随后用原Gateway打开短暂session、验证当前绑定epoch和原schema。Submit使用原strategy_order_id；Cancel使用原oms_order_id和显式`__ACCOUNT_SCOPE__`标记，再由原Gateway核对订单归属。当前原始peer由可信启动方在验证接口中提供；尚未接上公共SO_PEERCRED采集，不能将这些测试宣称为上游不可伪造身份验收。
2. **单入口与一次校验。** 仅在显式`submission_ipc=True`候选中，私有UDS拒绝Submit/Cancel，保留原只读接口；唯一业务WRITE通道进入同一Book。进程内授权lease绑定同一帧/短暂session，每次实际执行仍验证当前session与原账户锁内规则，schema不反复执行。成功或任何未知执行异常消耗lease，禁止重复运行；只有原canonical冷准入严格pre-commit异常可恢复，最多三次lookup/四次执行尝试，不增加旧上限。resolver、session和lease不跨IPC。当前不承诺远程排队取消：发出后若未知，不能冒充NOT_STARTED或自动重发。
3. **额度及执行边界。** 当前接线器为单调用线程、单inflight的同步qualification port；无额外等待线程或业务任务队列。每笔必须借用调用方原fair-ready WRITE credit，完成待交付仍占额，显式ack当前授权或dispose后才release。过期/错误schema/旧epoch仅在owner确认handler未进入时返回NOT_STARTED；STARTED后结果未知、错回复/CRC/关联/回执越过发布水位、断链或超时均fence。owner close前要求通道已经交付/处置且drained；WRITE线程未退出时不得关闭其仍引用的Book/Journal文件。该同步port不能直接阻塞公共reactor，公共异步状态机为下一包。
4. **源证据不改名冒充。** completed明确标记`source_anchor=RECOVERED_PREFIX`：摘要只证明同代启动时已校验的恢复前缀，另携带同Journal最新已发布LSN并核对原真实receipt LSN不超过它。它不是该最新LSN的全量内容哈希，也不是SSD FULL或断电恢复证书。旧CURRENT_VERIFIED_PREFIX协议用例保留；同一ledger不接受两种anchor中途切换，恢复摘要须与ready完全相同。这样不在每笔热路径重新扫描或哈希整个Journal，也不削弱原Book先commit后receipt顺序。
5. **观测和默认行为。** IPC使用单独单producer trace，schema为`consumer-owner-ipc-trace-v1`，明确不是公共首读/接纳trace；记录前端send、owner领取/开始、原Journal commit段、冷查段及回执返回的可关联时间。脱敏只用request ID哈希/generation/serial，不写账户、游标、帧或异常文本。新IPC trace从原trace总预算划出1/8，原读trace相应减额，CPU/6GiB/FULL/入口上限均不增加。正常服务开关关闭，旧E3b私有完整验证路径和普通listener默认行为不变。

本地新业务/lease/协议/原Gateway与listener、原owner及长运行候选/队列回归144 passed、28项Linux显式skip、186 subtests passed；变更模块Pyright/Ruff通过。Linux必须实际证明：继承WRITE提交、私有第二入口被阻断、过期和旧epoch无副作用、冷幂等重放、未拥有订单Cancel保持原冷读边界拒绝、伪造原peer/scope/请求关联拒绝；并用test-only spawn bootstrap在原canonical handler返回后、IPC completed前`os._exit(77)`，验证前端保留未知占额、禁止自动恢复/重发、仅手工恢复后从源找回原单且不重复。所有原始runtime/source/身份/trace仍受控留存，不提交密钥。

首轮Linux记录`20261023-oms-owner-e3c1-validation`：320 passed、2显式skip、189 subtests passed、1 failed（70.09秒）。失败是新增测试预期未知订单Cancel应返回NOT_FOUND，但原`OrderBookConsumerOperationsV2.get_order`明确禁止WRITE上下文触发冷SQL回退授权，因此原Gateway/Dispatcher返回INTERNAL_ERROR且不提交事实；不是IPC授权旁路。修正测试保持该既有fail-closed错误，不放开SQL、不把未知或已退热单升级为可撤能力；冷单撤单未在本包实现或验收。原始失败日志/运行目录保留，修正后完整组合待重跑。

修正后`c656a1e`完整Linux321 passed、2旧模式专属skip、189 subtests passed（69.87秒），归档`20261023-oms-owner-e3c1-r2-validation`。补强提交后丢回复夹具为**确定故障点**：原handler返回后，先观察本笔Mock终态和cold FULL receipt退热完成，再在IPC completed之前os._exit(77)；恢复必须恰为4 facts，重试仍是同一OMS ID且最终不增facts。原先立即终止可能落在2–4 facts任一点，不将这种不确定边界当统一证明。2 facts尚active时若恢复耗时超过原1秒Mock deadline，应保持fail-closed，不能改旧decision时间、提高deadline或冒充幂等重试成功；该更早边界及全部公共故障矩阵仍待后续。补强不修改生产代码、Mock期限或风险门禁，需在最新test revision重新Linux验证。

`53b2b41`确定故障点完整Linux321 passed、2 skip、189 subtests passed（69.98秒），证据`20261023-oms-owner-e3c1-r3-validation`。收尾对照原BoundedColdAdmissions桥接合同，补齐新同步冷恢复对resolution完整key和checkpoint≤256KiB/不可变bytes的显式检查，非法结果必须fence且不能进入下一次handler；未扩大旧缓存或重试。新增两项负面测试，本地本模块18 passed、6 Linux skip、Ruff/Pyright通过；该追加边界需在最新revision完整Linux组合再验证后，才能标记本子包验收通过。

**E3c1最终本包归档：`0e116b3`。** Linux完整323 passed、2旧global模式不适用scoped专属skip、189 subtests passed（69.94秒）。新业务通道六个真实spawn场景全部执行，无专属skip；确定4 facts提交后/IPC completed前exit=77，前端明确unknown且保留原额度，手工重建同Journal/新generation后查到原单，重复请求同OMS ID、最终仍恰4 facts。伪造peer/scope/request ID/business key均无成功回执，进程失败后手工恢复水位0；schema/epoch/deadline前置检查、第二私有WRITE入口拒绝、冷幂等与原Cancel安全拒绝、关联错误/水位越界/恢复摘要错误和丢回复保护均有测试。

证据根`/var/lib/tradecore-deployments/20261023-oms-owner-e3c1-r4-validation/`保留JUnit、日志、acceptance/hash；`owner-runtimes/`登记12个原始私有运行目录，逐源Journal文件记录SHA-256，提交后未知场景的实际恢复coverage水位核对为4。初轮失败及其原始目录不覆盖，expected非零owner退出逐项断言，不能把测试unit的success误称为故障owner正常退出。记录不公开私有identity，不宣称掉电、完整公共故障矩阵或性能通过。

变更模块静态检查通过；每包已push并核对dev。仅服务器Git快进源码和有限资格测试，未安装新wheel、未切换正常公共路径、未启动Adapter、未改CPU/内存/SQL/队列总上限。最终三个正常服务active/running、Result=success、NRestarts=0，qualification inactive。下一包仍是E3c2公共reactor非阻塞业务IPC与完整读交付fence，不以本同步port替代；HOT/LOW、2K＋2K、S4、十二小时及READY尚未通过。

后续E3c2才接公共reactor的非阻塞send/read、原身份采集、共享ready/未交付额度和只读交付fence；不得将本同步qualification port塞进公共reactor或将E2共享GIL线程候选当性能选型。完整公共端、故障及HOT/LOW/三轮2K＋2K通过前，不部署默认新路径、不解除S4/READY门禁。

#### 2026-10-23 E3c2：公共非阻塞前端、唯一进程owner与读交付隔离

状态：**公共WRITE/READ进程owner的实现、功能与故障模块验收通过；候选性能未满足完整发布门禁，默认关闭，E4/S4/S8/十二小时仍未验收。** 本节记录最终事实及中间否决，不能用最后一次成功覆盖先前故障证据。

1. **公共WRITE接线。** `b8b4fbd`将公共reactor接到继承的owner WRITE通道：公共端先取得真实`SO_PEERCRED`、完成原XTC2 framing与有界fair-ready credit，再以非阻塞状态机发送不可变信封；owner仍是唯一Book、Journal、Mock及冷恢复写者。排队、已开始和完成待交付共用原额度；断线、超时、半帧、错generation/serial/hash、提交后丢回执仍进入unknown/fence，禁止自动重放。`96d1897`把资格测试改为正式单请求客户端，避免测试peer形成另一套协议。
2. **权威READ接线。** `a6f8cb5`把Snapshot、GetOrder、CreateReplayPartitions、PollEvents等只读能力送入owner控制的读IPC，前端不创建私有Book或SQL写者；READ/WRITE共享原公共身份、当前授权、generation和容量合同。独立owner生命周期、bootstrap失败tracker关闭、公共源commit故障矩阵及源关闭证书由`f1934e6`、`89d214b`、`430e104`、`af2950d`补齐。Linux组合由335 passed提升到359 passed，2个skip均为旧global模式不适用的scoped专属用例；证据分别在`20261023-oms-owner-e3c2-r2-validation`、`20261023-oms-owner-e3c2-fault-r2-validation`及`20261023-oms-owner-e3c2-read-validation`。
3. **被否决的调度微调。** `7a82980`只在原128总credit内允许多个已编码WRITE在途，最终Linux 360 passed、2旧模式skip、189 subtests passed；它不增加并行writer。`7a25989`取消初始READ多余grant往返。曾尝试把继承WRITE放到owner CPU（`dee7ab1`），因与owner主循环争用被`1a9048c`明确回退；`2e4d30a`只固化进程owner GIL实验开关和证据，未成为默认。上述修改不能用来声称柜台并行或扩大队列。
4. **S6A客户端签名checkpoint并发。** `0633e15`将replay读取隔离在既有READ credit内；`bb8cf99`允许压测客户端对服务器签发的独立partition cursor并发推进，每个partition仍串行确认自己的checkpoint，禁止切割或伪造签名cursor。最新Linux工具回归50 passed、1个非本场景显式skip（11.40秒），证据`20261023-s6a-bb8cf99-r3-validation`。该包证明工具和合同，不证明单权威服务端cursor已经横向扩展；因此S6A模块通过、S6阶段仍未采用。
5. **独立只读响应进程。** 直接让WRITE owner同时执行高频序列化/分页读取会重新争夺其GIL和reactor。`33d21d8`新增只读、lease-free的已发布Journal source；它只接受完整commit frame并验证版本、长度、CRC/hash，不持有writer lease。`3b9ae90`发布固定read fence，绑定Journal ID、generation、恢复摘要及投影观察水位；`86774e5`在原owner子树内增加单独READ响应进程，`--isolate-owner-reads`默认关闭。前端只向该进程交付已授权READ，WRITE仍只有原owner；read进程丢失时公共服务fail-closed且不自动重建，不影响Journal写权归属。`82835f3`、`4dbf337`、`7a9fff9`补齐kill/EOF/旧代、直接协调器回收和真实PollEvents重放。
6. **fence根因及修复。** 首次混合A/B中隔离模式的请求负载全部完成，但事件观察立即返回`CURSOR_RECOVERY_UNAVAILABLE`。根因不是Redis、队列背压或事件丢失，而是fence错误要求`projector observed LSN == 当前 committed LSN`；持续写期间合法状态本来就是`projected <= observed <= committed`。`77c13e8`改为该单调有界关系，仍拒绝投影越过观察前缀、Journal/generation/hash改变或任何水位回退，没有放松完整frame及恢复摘要校验。

**最终功能/故障验收。** `77c13e8`本地Ruff/Pyright通过、22项定向单元通过；服务器fence及真实隔离READ回放8 passed。最终显式Linux owner组合在6GiB、`KillMode=mixed`、CPU 64–71下执行69 passed、0 skipped（105.11秒），覆盖writer lease、真实spawn/kill/EOF/旧代stop、公共Snapshot与PollEvents、read进程丢失fail-closed及手工恢复。有效证据根为`/var/lib/tradecore-deployments/20261023-owner-read-plane-77c13e8-full-r3`，JUnit SHA-256=`2facd242169371621efd5644960f9d96bc922ee573b705e33b4b67a8a181d66e`，日志SHA-256=`1abe389cfab4d148403dbf4b7f2b0e7694ec88ceaf87f625041819e04780ac94`。此前57 passed/12 skipped的漏开资格变量轮，以及12项因未建立evidence目录失败的轮，均保留为编排无效证据，不计入验收。

**HOT同口径A/B。** 每种模式三轮，每轮1,000预热＋10,000单inflight，所有10,000请求均成功且唯一LSN完整：

| 模式 | 三轮P50，ms | 三轮P95，ms | 三轮P99，ms | 三轮TPS |
|---|---|---|---|---|
| 原owner内读 | 0.421555 / 0.428035 / 0.429567 | 0.927976 / 0.997600 / 0.981016 | 2.822355 / 2.905359 / 2.822096 | 1870.19 / 1825.46 / 1834.50 |
| 独立READ进程 | 0.417009 / 0.423919 / 0.424229 | 0.942028 / 0.973625 / 0.961757 | 2.984317 / 2.928324 / 2.924569 | 1882.06 / 1836.73 / 1840.30 |

隔离模式没有可认定的低压明显倒退，但双方均未满足绝对HOT P95≤0.30ms/P99≤0.50ms。汇总`/var/lib/tradecore-deployments/20261023-e3c2-readplane-4dbf337/hot-ab-r4-summary.json`，SHA-256=`c4aa61f74286ffed85ee1e77717c8277a61723240c920d4501835cf34674c4bd`。该结果只能支持“隔离不明显伤害HOT”，不能把HOT标为通过。

**修复后10秒2K Submit＋2K Snapshot混合三轮。** 每轮各计划20,000笔，事件目标40,800；CPU、6GiB、FULL、256/512、1,024 seed及原队列均不变：

| 轮次 | Submit完成/窗内 | Snapshot完成/窗内 | 唯一checkpoint事件 | 最终排空 | 请求/事件错误 |
|---|---:|---:|---:|---|---|
| R1 | 20,000 / 20,000 | 20,000 / 20,000 | 40,800 | 达到 | 0 / 0 |
| R2 | 20,000 / 20,000 | 20,000 / 20,000 | 40,800 | 达到 | 0 / 0 |
| R3 | 20,000 / 19,998 | 20,000 / 20,000 | 40,800 | 达到 | 0 / 0 |

三轮最终40,000个请求全部成功、零unissued/背压/传输错误，PollEvents不再报`CURSOR_RECOVERY_UNAVAILABLE`，每轮事件唯一性和最终排空均闭合。R3有2笔Submit越过10秒窗口，因此“三轮窗口门禁”仍为false，不能重跑挑选或把最终成功改写成窗内成功。修复后汇总SHA-256=`aa2dc36455ac9f2dc604717f3dc64468bd23c0649b23e81e59100333dca6e8e7`；总验收清单`/var/lib/tradecore-deployments/20261023-e3c2-readplane-77c13e8/acceptance-summary.json`，SHA-256=`eeae6782249a5bb0fe19a4807c0140f38eff8462cce184d1ac3c4f09905f2b86`。修复前混合汇总保留并标记为fence缺陷导致的无效性能证据，不覆盖。

**部署结论。** E3c2的WRITE/READ所有权、身份、故障和恢复功能包记为`MODULE_ACCEPTED`；独立READ进程仍为默认关闭的资格候选，`PERFORMANCE_NOT_ADOPTED`。精确资格wheel SHA-256=`4a02fd2e55164cd3d9aa42751cf43cbd89882bf4cd58666b6b64d229cc31ea2e`，未替换正常生产wheel、未调用Adapter/EMT/QMT。三个正常服务均active/running、Result=success、NRestarts=0。由于HOT绝对门禁未过、混合R3有2笔窗后完成、且本轮只是10秒诊断而非完整30/60秒与十二小时资格，整体继续`IMPLEMENTED_NOT_ACCEPTED`；不得解除S4/S8/十二小时或交易`READY`。

#### 2026-10-24 E4：30秒共同窗口、owner交接归因与否决实验

状态：**30秒共同窗口的请求/源/消费最终完整性已证明；4包有界owner交接候选的功能/故障模块通过且延迟显著改善，但严格窗内门禁仍只有2/3轮通过，未进入60秒HIGH。** 所有轮次均保持同一6 GiB、CPU 64–71、FULL、256 transaction batch、512 facts、1,024 admission seed、原队列/credit及独立READ进程；每轮新namespace、60,000 Submit＋60,000热页，Adapter/EMT/QMT均未启动。

1. 首次runner把负载unit的预期非零诊断退出误判为基础设施异常，导致未执行正式final drain。该轮保留在`e4-high30-r1-summary.json`，不计入验收。修正runner仅改为`systemd-run --no-block`并等待unit终态，不改业务/资源参数。
2. 原`77c13e8`三轮均最终完成60,000＋60,000请求、零技术错误、零漏发，且每轮120,800唯一事件最终排空；R1/R2/R3 Submit scheduled P95分别为27.542/22.348/18.579 ms，P99分别为38.371/37.323/30.769 ms。仅R3满足HIGH提交P95≤20 ms，故三轮门禁失败。摘要`/var/lib/tradecore-deployments/20261023-e3c2-readplane-77c13e8/e4-high30-r2-summary.json`，SHA-256=`8ff4e05f7cbc2e7584d207ab6586f5f89a865508139fd0561ac7adfcfbc33d19`。
3. 逐请求客户端raw与owner IPC trace按request digest配对60,000/轮。客户端send→owner receive是主等待：R1/R2/R3 P50为4.203/3.943/3.905 ms，P95为20.752/16.538/11.917 ms；owner receive→reply P50仅0.237/0.232/0.235 ms、P95仅0.728/0.682/0.680 ms；owner reply→客户端validated P95为4.427/4.174/3.853 ms。三轮Journal均3次轮换，cold SQL/Journal/GC量级相近，owner trace queue零drop且峰值仅39/59/42，排除Redis、SQLite单点退化、Journal容量或入口背压作为R1/R2差异的主要解释。
4. `6a516fa`实验在`offer`时对可写本地socket执行一次非阻塞send，真实Linux owner功能/故障38 passed，JUnit SHA-256=`7af560b81ce77d5f2830e811387ee57302361d45619cef0cccdea8bcf8b061b6`。但同一reactor turn可连续填充原128 credit，三轮Submit scheduled P95放大到227.148/143.131/214.434 ms，且各轮仅59,659/59,658/59,622笔窗内完成；虽然120,000请求最终均成功且120,800事件最终排空，该实验明确`PERFORMANCE_REJECTED`。摘要SHA-256=`1d92fe4cf493115f4a1c45d90c8767ac3c2e8821b9eb926630a09a2a28758c9a`。
5. `4256735`增加每reactor turn最多一次eager handoff，Linux功能/故障仍38 passed，JUnit SHA-256=`4181ebb5ce2e821cba38e467d1ed9eb6b0d46edca615ae95ae29674c2cb5022a`。R2/R3分别P95 18.596/18.842 ms且请求全窗内，但R1 P50/P95达211.550/235.799 ms，Submit仅59,607笔窗内；该版本三轮不稳定，仍`PERFORMANCE_REJECTED`。摘要SHA-256=`7de58e1236a4b330737bb89cd2117c198524caac7e25f0bc6633e3380f1be7bc`。不能删除R1或重跑挑选R2/R3冒充通过。
6. 两个实验已分别以`9603872`、`0cc8882`回滚并push，当前dev恢复E3c2原owner交接算法；失败部署只保留为私有资格证据，未替换正常生产wheel。测试结束后三个正常服务均active/running、Result=success、NRestarts=0。
7. `941a4ac`在不扩大16 GiB总trace预算的前提下，把原私有空listener的请求trace预算留一段、其余预算移给真实公共frontend；公共trace在受控关闭前必须完成drain、manifest/hash-chain和零drop核验，否则lifecycle不能成功。短生命周期实测1,101/1,101条写入、drop=0、writer正常关闭。本地35 passed、25项Linux专属显式skip、6 subtests passed，Ruff/Pyright为0错误；精确资格wheel SHA-256=`37f2f334368e68c63353897cc030bb7f40ea9e0bbcfb39a021fbdb3816c9e973`，未替换正常生产wheel。
8. 使用上述公共frontend trace重新执行同资源三轮30秒，每轮60,000 Submit＋60,000热页最终全部完成，且Submit的client raw、frontend trace、owner trace均60,000/60,000精确配对；R1/R2/R3 Submit窗内完成59,901/59,986/60,000，scheduled P50为8.905/8.774/8.446 ms，P95为44.827/42.107/32.735 ms，P99为54.166/50.495/40.567 ms，三轮均未通过。full-trace会增加公共进程的JSON编码/异步writer GIL负载，因此本轮不能替代原无公共trace性能基线，也不能据其绝对值宣称生产退化；但同一请求的阶段因果及60,000条覆盖完整，可用于定位等待归属。摘要`/var/lib/tradecore-deployments/20261024-e4-frontend-trace-941a4ac/e4-high30-r5-summary.json`，SHA-256=`7c58302eaac21ba1d3e70b8cdbf8d99e97abf84ffddea1b77166623b98e93374`。
9. 三轮Submit阶段P95（ms）如下；owner业务本体保持亚毫秒，主要等待均位于公共frontend/reactor及其owner传输的调度边界。`owner started→schema`的采样赋值顺序可出现数微秒反向，未作为可相加区间；表内其他阶段均只统计非负、逐笔同request配对值。

| 阶段 | R1 P95 | R2 P95 | R3 P95 |
|---|---:|---:|---:|
| accept→first byte observed | 13.904 | 11.247 | 3.501 |
| first byte→完整帧 | 0.002 | 0.002 | 0.002 |
| 完整帧→decode | 0.007 | 0.007 | 0.007 |
| decode→dispatch | 12.217 | 11.845 | 10.082 |
| dispatch→owner offer | 0.047 | 0.047 | 0.046 |
| owner offer→owner receive | 13.371 | 13.007 | 12.029 |
| owner receive→owner started | 0.039 | 0.039 | 0.039 |
| schema→handler finished | 0.438 | 0.400 | 0.339 |
| handler finished→encode | 0.005 | 0.005 | 0.005 |
| encode→owner reply send | 0.361 | 0.328 | 0.291 |
| owner reply send→frontend receive | 7.378 | 7.186 | 6.462 |
| frontend receive→send finished | 0.012 | 0.012 | 0.011 |

10. 结论不是“Journal/SQLite继续减负”：完整帧解析和dispatch→offer均只有几十微秒，owner业务P95小于0.5 ms；最重的四段是公共socket接纳后的调度、decoded-ready等待、单owner流式IPC交接和owner回包在公共reactor中的领取。下一候选必须保持单OMS writer与原128总credit，但把WRITE包按**小批有界、同turn一次发送**交给owner，先以4包/64 KiB硬上限验证，禁止一次填满128 credit；若仍无法连续三轮通过，再进入多frontend进程＋单acceptor FD移交设计。任何候选都须保留逐笔身份、serial、unknown/fence、回复顺序、READ公平和故障矩阵，失败立即回滚。
11. `296787a`实现每reactor turn最多4个已借用原WRITE credit的owner包，以单次`sendmsg`向内核交付，单turn收发总量各≤64 KiB；回包侧同turn最多领取4个terminal，逐帧仍验证header、CRC、generation、serial、source fence和公开响应关联。总credit仍128，owner仍单writer，未增加业务队列、线程、并行Journal或跨请求缓存。`f732ee8`把公共full trace改为显式`--frontend-request-trace`，默认关闭；关闭时不创建writer，生命周期明确记录trace disabled，避免用测量开销污染正式性能判定。
12. 首版4包候选的Linux真实进程资格28 passed（6 GiB、CPU 64–71、`KillMode=mixed`）；随后`3a90481`把第4包到达时立即交付内核，并以turn计数保持本轮剩余WRITE关闭，禁止继续填满128 credit。最新Linux同组仍28 passed，JUnit SHA-256=`69ff95d7324b96308bb5571ea93f1500383bddeb899bb5e2d0c3d7c47576f81f`。精确资格wheel SHA-256=`4fd89be4f3238e5b0ff7cb13d7b8b5d25f37ec257a99a5694968da7732581889`；未替换正常生产wheel。
13. 首版`f732ee8`的full-trace单轮120,000请求全部窗内完成，Submit scheduled P50/P95/P99为5.599/13.410/26.952 ms；60,000条三层配对显示accept→first-byte P95=1.775 ms、decode→dispatch=1.538 ms、owner offer→receive=7.578 ms、owner reply→frontend=0.672 ms，证明回包领取已从旧6.5–7.4 ms降到亚毫秒，owner业务P95=0.268 ms。摘要SHA-256=`9eaaa544b4358650c27d399263a79a292f6dc13760be040c13f62f0b9f967473`。其关闭frontend trace的正式三轮Submit P95为14.852/14.664/14.778 ms，R1/R2全窗内，R3有4笔在窗口后0.044–0.941 ms完成；该4笔均是窗口结束前恰10 ms释放的最后批次，不是漏单、拒绝或持久化失败。摘要SHA-256=`ce41f9422047413fa032365ede6fc5d1841ca42bce28ebbb602eb3695fac674f`。
14. `3a90481`即时4包交付的full-trace单轮Submit P95=14.311 ms，但Submit 59,998/60,000、Snapshot 59,989/60,000窗内，最终均完成；owner offer→receive P95=7.922 ms、owner reply→frontend=0.676 ms，没有证明相对首版进一步改善。摘要SHA-256=`c8afcc4ec6a16e8f634943409383f9ea4748535e28a1e2a846a42b4978ffe6bd`。关闭frontend trace的正式三轮Submit P50为5.591/5.603/5.640 ms，P95为13.361/14.221/13.222 ms，P99为27.034/27.378/26.526 ms；R1/R3全窗内，R2有5笔Submit窗后完成，Snapshot三轮均60,000/60,000窗内。所有请求最终成功、无拒绝/漏发，120,800事件各轮最终排空；但严格标准按三轮判FAIL，不能重跑挑选R1/R3。摘要SHA-256=`f525ab5182cc6c0f0cefd200d495712ac00115c78d07e718f142c91c1a253efd`。

**门禁结论与后续。** E4的30秒“最终请求及事件完整性”和4包IPC功能/故障模块通过，Submit P95已稳定低于20 ms；但严格窗内门禁仍只有2/3轮通过，HOT绝对门槛、LOW、60秒HIGH、背景公平、突发/降压、正式资源及证据总验收均未通过。4包候选保留在代码中且仅随默认关闭的进程owner候选生效，不部署正式生产wheel。下一阶段进入多frontend进程＋单acceptor FD移交的设计与实现：acceptor只接受连接并传递原socket FD，固定frontend worker独占自己的selector/GIL和owner通道，单OMS owner仍串行写入；必须保留原SO_PEERCRED、共享128总credit、按serial有序terminal、unknown/fence、READ公平与受控关闭。不能继续只调GIL/投影batch、无限增加credit、并发写同一OMS账户或修改末批时间来“通过”。三轮通过前不运行60秒，不解除S4/S8/十二小时或交易`READY`。

#### 2026-10-24 E5a：单acceptor FD移交与跨进程共享credit底座

状态：**FD移交、安装确认、共享128总credit及单worker真实公共链路模块验收通过；多frontend owner broker、完整多进程生命周期和性能尚未实现，不得标记E5/E4/S4通过。** 本包默认没有CLI入口，不替换正常公共服务，不增加OMS writer、队列或credit。

1. `d23b831`增加Linux `AF_UNIX/SOCK_SEQPACKET + SCM_RIGHTS`固定协议。acceptor传递一枚仍未读取业务字节的已连接`SOCK_STREAM` FD及固定generation/serial/accept时间/原`SO_PEERCRED`；worker收到后独立重新读取`SO_PEERCRED`并逐字段核对。信封固定长度、版本、CRC，截断、额外FD、未知ancillary、错误socket类型、伪造peer均fail-closed；拒绝路径关闭内核安装的全部FD，不能泄漏或继续接单。
2. `e46d9a4`增加每worker一个sent-but-not-installed槽位、轮询分发及严格ACK。ACK不带FD，必须匹配原generation/serial；错误、重复、无pending ACK立即fence。没有第二个Python连接队列；所有槽占用时`offer`明确返回backpressure且未移交的FD仍归acceptor处理。`82098d9`增加worker侧listener facade；同一份allow-list在worker重复认证，只有连接进入worker受控selector/ready或显式overload生命周期后才ACK。原直接listener的hook为空操作，因此默认路径行为不变。
3. `aae7256`增加spawn树内部共享credit：容量硬上限1..128、最多64个固定worker，global held必须等于各worker held之和。正常释放必须由原worker完成；worker死亡/错释放只允许fence整个候选，禁止回收额度后继续处理不明写。服务器四个spawn进程竞争128额度实测合计只能取得128，正常释放后global/各worker全部归零。
4. `cfdaa06`把上述listener作为`build_process_owner_frontend`的显式可选输入，默认仍使用原公共socket。真实公共资格用一个独立acceptor把客户端FD移交到完整frontend reactor，再经过原WRITE/READ IPC进入唯一OMS owner；acceptor与worker均核验原peer。该用例不是多worker性能证书，只证明现有完整业务路径可以安全消费移交FD。
5. 首次Linux端到端轮在第一个FD安装成功后，worker为同一selector turn继续批量accept；此时packet队列为空应像普通非阻塞`accept()`返回`BlockingIOError`，封装却误转为协议失败。失败JUnit保留于`20261024-e5-fd-handoff-cfdaa06/junit.xml`，SHA-256=`45bb5785c0a8958ddf1546f44efd6858963fc5fe395338273cd26acf95aa1572`。`c2c4429`恢复该正常空队列语义，并增加非阻塞空通道回归；不放松截断、EOF或坏ancillary拒绝。
6. 第二轮Submit已真实返回`ACCEPTED`，但用例立即读取依赖投影水位的Snapshot，投影尚未追平使权威read-plane按原合同fail-closed。该失败JUnit保留于`20261024-e5-fd-handoff-c2c4429/junit.xml`，SHA-256=`857757f89d0693c4109f8fb3b415379503c32ca9e4a4a7b309cab01396045c67`。最终用例改为不依赖异步投影追平的`ListAccounts`验证同一READ通道；原Snapshot/Replay投影一致性用例保持不变，不把投影等待伪装成FD缺陷或降低其标准。
7. 最终revision `b8dd758`在`tradecore.slice`、CPU 64–71、6 GiB、`KillMode=mixed`、项目CPython 3.10下通过19 passed/0 failed/0 skipped（2.04秒）。覆盖固定信封/CRC、原peer和业务字节保持、伪造与额外FD、接收端泄漏、ACK、单槽backpressure、错ACK fence、空非阻塞队列、共享credit本地/真实spawn、真实Submit及权威READ。证据`/var/lib/tradecore-deployments/20261024-e5-fd-handoff-b8dd758/junit.xml`，SHA-256=`21a050c5a587c9854dc35c399ba3b7eb650c8fbf6ff962523697b9e7ddf87d3e`；内存峰值93.5 MiB，测试unit成功退出。

**尚未完成且下一包不得省略：** E5b需实现固定多个frontend进程的受控spawn/CPU归属和唯一acceptor监督；每个worker独立selector/GIL，但WRITE/READ必须经一个共享额度协调器进入现有单OMS writer，不能把128复制成`worker_count × 128`，也不能让多个通道并发调用同一可变Book/Journal。协调器必须重编号并反向关联跨worker serial，terminal仍按唯一owner执行顺序验证，客户端断开后的unknown/fence、READ四rendezvous公平、worker/acceptor/协调器kill与受控drain均须专项验收。完成E5b功能/故障矩阵后才运行HOT/LOW和30秒2K＋2K三轮；未连续三轮通过前不进入60秒门禁。当前没有安装资格wheel或切换正常服务；`xuanling-tradecore`、fault-supervisor和Consumer UDS均active/running、`Result=success`、`NRestarts=0`。

#### 2026-10-24 E5b-1：共享credit接入frontend owner端口

状态：**模块通过，尚未接入多frontend公共服务。** `8472767`把E5a的spawn-safe共享额度接入`ReactorOwnerPortV1`真实请求生命周期。构造时必须同时提供共享credit与固定worker ID；调度提示和最终`try_acquire`共同限制全部worker合计最多128。只有terminal已验证且公共响应完成交付/显式处置后，才同时释放原fair-ready WRITE credit与该worker的共享credit。owner EOF、CRC/顺序/关联错误或执行超时会fence共享对象且保留未知工作的占额，禁止把失联worker的额度回收后继续交易；正常stop还要求本worker关联表、原ledger和共享worker计数同时归零。

本地定向回归20 passed，Ruff/Pyright为0错误；push后服务器Git快进到精确revision，在`tradecore.slice`、CPU 64–71、6 GiB、`KillMode=mixed`中复验20 passed、0 failed/skipped（0.61秒），systemd结果success，峰值2.0 MiB。测试覆盖两个独立owner端口竞争同一3-credit硬上限、完成待交付继续占额、释放后另一worker可取得额度，以及owner断链使共享credit保持held并全局fence。该包没有安装wheel、没有更改正常服务或调用Adapter。下一包必须实现中央WRITE broker的跨worker serial重编号、STARTED/terminal反向关联和有界回包；在broker完成前，不把共享额度竞争失败接到已经消费帧的公开dispatch，避免形成半成品丢帧路径。

#### 2026-10-24 E5b-2：中央WRITE broker与跨frontend serial关联

状态：**WRITE broker模块及现有真实owner/FD链路回归通过；尚未组成多frontend服务。** `d910dfd`增加单线程、有界`FrontendOwnerWriteBrokerV1`。每个frontend仍以自己的递增serial工作；broker核验generation/scope、该worker确实持有共享credit后，分配唯一中央serial并进入同一个`ReactorOwnerPortV1`，不存在第二个writer。owner的真实STARTED和terminal均经过原ledger验证，再换回原frontend完整identity；不根据terminal伪造STARTED。每个请求最多形成STARTED/terminal两个有界回包，中央terminal credit只在完整写回来源frontend后释放，来源frontend仍须等公共客户端交付后才释放共享credit。错序、无路由、额度不匹配、任一通道EOF或发送错误均同时fence owner与共享credit，不重连、不重放、不回收未知工作。

本地定向22 passed，Ruff/Pyright为0错误。push后首轮Linux中22个协议测试通过，但7个真实owner用例因编排漏建强制0700 `owner-runtimes`证据子目录而在启动前被共同门禁拒绝；该轮只作为无效编排记录，不是代码失败。补齐目录后原样重跑29 passed、0 failed/skipped（10.27秒），systemd success，峰值157.2 MiB；JUnit位于`/var/lib/tradecore-deployments/20261024-e5b-write-broker-d910dfd-r2/junit.xml`，SHA-256=`d7fb879f8a9406a026fb2133c6d3290ecca1be1f5e1d146e5acc6da80f8c65c5`。真实FD handoff、公共WRITE/READ、独立read进程、断链/kill及源commit边界均连带通过，三个正常服务仍active。下一包为READ broker及固定frontend spawn/监督；在READ四rendezvous、公平性、全子进程CPU和关闭顺序闭合前不启用公共候选或做性能宣称。

#### 2026-10-24 E5b-3a：跨frontend READ总量与rendezvous守恒

状态：**共享READ容量模块通过，READ broker尚未完成。** `b15f572`使frontend WRITE与READ共同借用同一全局128总credit，READ还必须额外借用独立的全局4-slot credit；每个worker本地四通道只用于协议握手，不再形成`worker_count × 4`可执行/等待容量。两项共享额度都在offer时非阻塞取得；任何一项或原fair-ready reserve失败会原子回滚。只有权威GRANT交付后由frontend发出RELEASE、且RELEASE完整写入broker通道后，才释放本地lane、全局READ slot与全局total。READ协议/通道失败同时fence两项共享额度，未知工作不回收。WRITE端stop改为只核对自己持有的serial已清空；最终全局drain由未来统一supervisor核验，避免READ仍合法占额时单组件误判。

本地READ/WRITE/共享额度定向29 passed，Ruff/Pyright为0错误。push后Linux在原6 GiB、CPU 64–71、`KillMode=mixed`下执行共享READ、WRITE broker、真实公共owner/FD链路及故障回归38 passed、0 failed/skipped（10.28秒），峰值157.5 MiB；JUnit为`/var/lib/tradecore-deployments/20261024-e5b-read-credit-b15f572/junit.xml`，SHA-256=`9383de740478a80ea51b710a2896fee619022113b74a8e5a0a6732a5c1c6aaf1`。下一包在该硬容量内代理REQUEST/GRANT/CHECK/RELEASE并反向映射worker identity；不得为简化代理恢复每worker独立四个owner执行slot。

#### 2026-10-24 E5b-3：跨frontend READ broker与释放确认

状态：**READ broker模块及现有真实owner回归通过；固定多frontend进程supervisor仍未组成。** `a9c79f7`实现`FrontendOwnerReadBrokerV1`，各worker本地仍有四条非阻塞协议通道，但只有共享4-slot取得者才可发送REQUEST；broker按worker/原serial保存精确路由、分配唯一中央serial并进入现有唯一`ReactorOwnerReadsV1`。中央权威GRANT携带的响应重新绑定原identity返回；公共socket部分发送前的CHECK必须核对旧digest，并在中央owner重新GRANT后才恢复交付。协议新增只用于broker私有链路的`RELEASED`确认：worker发RELEASE后继续持有本地lane、全局total和全局read slot；中央RELEASE真实写入owner通道、slot消失且broker把RELEASED完整写回后，worker才释放三层额度。因此不存在释放窗口中的第5个隐藏请求。

本地READ/WRITE/协议组合106 passed、1个既有平台专属skip，Ruff/Pyright为0错误。push后Linux以原资源执行broker、共享额度、协议全集及真实公共owner/FD/kill/commit边界回归114 passed、0 failed/skipped（10.86秒），峰值159.8 MiB；JUnit为`/var/lib/tradecore-deployments/20261024-e5b-read-broker-a9c79f7/junit.xml`，SHA-256=`2c92d22a90296e49e3da399f161da91130637d75b226135a70acf5ebb9495161`。正常三个服务均active，未安装候选wheel或调用Adapter。下一包必须把acceptor、固定frontend进程、WRITE/READ broker和唯一OMS owner纳入同一受控spawn/CPU/stop树，并做单worker/acceptor/broker/owner kill矩阵；模块测试不能替代该进程拓扑验收。

#### 2026-10-24 E5b-4a：固定spawn frontend worker生命周期

状态：**独立frontend worker启动/CPU/drain模块通过；统一acceptor+broker supervisor尚未完成。** `9dd9c0b`使原frontend builder支持两种互斥模式：既有直接owner模式保持不变；broker模式必须一次性提供ready、WRITE通道、四READ通道、共享total/read credit、固定worker ID和FD handoff，不允许缺项或叠加。`SpawnedFrontendWorkerV1`只允许一次spawn，不存在重启/重连；子进程构建自己的listener、selector/GIL和本地ledger，可绑定一个明确CPU。控制面仅发送READY/STOPPED生命周期状态；stop通过独立控制watcher请求reactor排空，只有WRITE/READ端口均`shutdown_verified`且子进程exit 0才算成功。超时或异常只能终止该候选并由上层fence，不能替换worker继续接单。

Windows本地因SCM_RIGHTS/Linux affinity显式skip，变更模块Ruff/Pyright为0错误。push后Linux在CPU 64–71/6 GiB cgroup中实际spawn frontend到CPU64，核对不同PID和单核affinity，再在零业务下受控drain；连同WRITE/READ broker及共享credit共8 passed、0 failed/skipped（0.85秒），JUnit为`/var/lib/tradecore-deployments/20261024-e5b-frontend-worker-9dd9c0b/junit.xml`，SHA-256=`8047be7cc5cb47b991936376b864961fd4c5e3fc5ae8e430f1a995934862b526`。下一包建立唯一acceptor和broker事件循环，启动多个上述worker并执行真实公共请求；在该包完成前不能把单worker生命周期测试称为多frontend验收。

#### 2026-10-24 E5b-4b：真实双frontend统一runtime

状态：**双frontend完整公共功能与受控drain通过；故障矩阵和性能尚未验收。** `0e24959`把唯一public acceptor、FD router、固定spawn frontend、WRITE/READ broker、共享128/READ4及唯一OMS owner组成默认无CLI入口的资格runtime。acceptor和broker同属一个监督线程，acceptor重复核对SO_PEERCRED，只把未读业务字节的FD轮询移交；每轮动态注册broker读写兴趣，不另建业务任务队列。检测任一worker退出、broker/owner协议错误或共享额度不一致即fence整组，不自动重启。正常关闭先停止accept，等待共享额度归零，再停止中央broker/owner端口、关闭各worker及父通道，最后验证唯一owner managed退出；任一未排空阶段拒绝干净证书。

首轮`0e24959`在启动前被既有CPU门禁拒绝，因为测试对isolated read plane只提供1个而非强制2个CPU；无子进程或业务请求实际开始，该轮保留为编排失败。`f8e55c5`修正为owner角色CPU64–69、两个frontend独占CPU70/71且不复用background核。push后真实Linux公共UDS经两个frontend完成8笔Submit `ACCEPTED`与8次权威`ListAccounts COMPLETED`，worker PID互异，随后全树受控drain，1 passed、0 failed/skipped（2.09秒），峰值196.0 MiB；JUnit为`/var/lib/tradecore-deployments/20261024-e5b-runtime-f8e55c5/junit.xml`，SHA-256=`323566125a0ae6322ef527f851a227a238c3d989547b74c27a212a9a354c73be`。该短功能轮不是性能或长期资源证书。下一包必须完成worker/owner/broker/acceptor故障与未排空关闭矩阵，并证明所有失败都不签发shutdown_verified、不重建generation或重放未知WRITE。

#### 2026-10-24 E5b-4c：统一runtime故障与公共功能矩阵

状态：**E5b功能/故障矩阵通过，可进入性能资格；仍未部署或采用。** `6016d7f`增加runtime只读故障状态及真实进程死亡用例。frontend SIGKILL由supervisor主动存活检查识别；唯一OMS owner SIGKILL由broker端EOF/协议fence识别。两类故障都保留原generation和原worker PID集合，不spawn替代进程、不重连broker、不重放未知WRITE，公共Submit随后失败关闭，且`shutdown_verified`保持false；测试finally只做候选containment abort，不冒充正常drain。

push后独立故障轮正常闭环、frontend kill、owner kill共3 passed（14.37秒），JUnit SHA-256=`5722c92f62a197cfd42b0c64ff7df933231e4e1700219019d86ef32879eb9d01`。随后同revision完整Linux组合44 passed、0 failed/skipped（24.63秒），覆盖双frontend公共Submit/READ、单worker lifecycle、FD handoff、WRITE/READ跨worker serial、共享total/read credit、CHECK/RELEASED、worker/owner kill、客户端断开、read-plane loss、源append/commit-word故障与受控drain；峰值198.5 MiB。总JUnit为`/var/lib/tradecore-deployments/20261024-e5b-functional-6016d7f/junit.xml`，SHA-256=`393f0beb110121ca9a9c30297f59fd23153ac7643a59a687027623cfa8c9f965`。正常三个服务仍active，未安装候选wheel、未调用Adapter/EMT/QMT。下一阶段才运行该拓扑的HOT/LOW和30秒2K+2K三轮；性能未通过前保持默认无CLI入口，不进入60秒HIGH、十二小时或READY。

#### 2026-10-24 E5b-5：broker进程隔离、高压定位与同turn有界推进

状态：**完整进程拓扑与故障矩阵通过；实测定位已完成；同turn优化已push且Linux模块验收通过，但尚未在新候选进程中做同口径性能复测。**

1. `46c4875`将owner WRITE/READ transport改为一次性转移给外部受监督broker；`b13b8dc`新增独立WRITE/READ broker进程和DRAINED→STOPPED两阶段关闭；`981a831`将它们接入固定8 frontend、单acceptor、单OMS owner的完整拓扑。`e556f5f`完成frontend/WRITE broker/READ broker/owner分别SIGKILL矩阵：全部保持原generation/PID集，不自动重建、不重放未知WRITE、不签发正常shutdown。Linux功能/故障合计44 passed，单独故障轮5 passed；证据JUnit SHA-256分别为`393f0beb110121ca9a9c30297f59fd23153ac7643a59a687027623cfa8c9f965`和`5d7e68b8d8e9326609bbd47f9c30fcbe0d30278cd47a18f89f867d2a824a0eb2`。
2. `038930d`将发生器并发上限从1,024提高至4,096，仍保留`workers×2`有界发布队列、最大2,000/op速率、FD和内存上限；Linux 20 passed，JUnit SHA-256=`85fa55e0c7e9e786724c5db2289667be07ee4794f09c2c9b9c3e7c14b89cb6e0`。
3. 1K总负载热轮中Submit/Snapshot各5,000全部完成且无transport错误；Submit scheduled P50/P95/P99/P100为3.559/5.747/28.000/38.830 ms，Snapshot为3.658/14.754/26.280/46.727 ms，因READ P95超标严格FAIL。3K总负载中每类6,537完成，无transport错误，但发生器在约4.25秒达水位；增至2,048 workers后每类8,615完成但只放大等待，未提高约3,000 total ops/s的服务平台，禁止以更多客户端排队伪造吞吐改善。
4. 3K同期CPU显示：owner reactor约76.7%、projection约59.0%、WRITE/READ broker各约33%，8个frontend各约13%。该结果证明旧单主进程100% GIL已解除，也证明当前不是任一frontend或broker CPU打满。owner IPC逐笔证据的最新8,714样本中，接收→开始 P95=0.053 ms、owner业务 P95=0.421 ms、响应编码 P95=0.007 ms、回包 P95=0.707 ms、owner内总P95=1.371 ms。因此不再盲目改Journal/mmap，下一安全目标是削减broker各阶段额外selector turn。
5. `827b412`在不改变单writer、共享128总credit、权威4 READ slot、顺序、unknown/fence或队列的前提下，让当前selector turn刚产生的非阻塞输出立即有界推进。WRITE每frontend最多推进STARTED＋terminal两个已缓冲协议包，READ每owner/frontend通道最多一次syscall；EAGAIN立即返回，不忙轮询、不产生新队列。本地8 passed/14平台skip，Ruff/Pyright通过；服务器Git已快进到精确revision，Linux broker/协议组合10 passed/12旧环境门禁skip，JUnit SHA-256=`af01530866f3d66f3542c2bf5366a6cae8f43cab0324ded48e5e951542baf437`。

**当前阻塞和后续。** 服务器现有性能候选是`e556f5f`运行进程；服务器checkout已到`827b412`，但新代码必须重建一个独立systemd候选才能做同口径性能对照。旧候选在3K/2,048-worker压力诊断后已按设计fence WRITE；后续试探的24,000笔Submit全部`IncompleteReadError`而进程仍active，这是失败封锁证据，不是可重用的性能轮。必须新建generation/全新Journal候选，禁止在被fence进程上重跑挑样本。当前保存的`xuanling`密码可SSH登录但已不是sudo密码，`fengyu`仅有既有受限验收命令，不能创建performance systemd unit。在获得新的受限部署命令或有效sudo凭据前，不得声称`827b412`服务器性能验收通过，也不得替换三个正常服务。

#### 2026-10-26 E5b-6：固定拓扑尾延时收口与3K连续三轮

状态：**固定多frontend候选的1K低压回归及3K短窗连续三轮通过；本阶段可以进入60秒HIGH和长期runtime组合门禁，但不能据此解除S4/S8、十二小时或交易`READY`。** 本轮只运行`NO_ADAPTER_BENCHMARK`，没有启动EMT/QMT、没有修改128总credit、4个权威READ slot、FULL投影、256 transaction batch、512 facts、1,024 seed、6 GiB或现有生产CPU分区。

1. `e5c3ac8`先把压测发生器的测量窗GC与服务端候选分离；旧拓扑的1K总负载三轮均通过，但3K总负载出现Submit P95约133–189 ms。逐笔trace证明owner业务P95约1 ms，主要等待发生在owner接收前。`a3ed9c3`在READY前执行一次`gc.collect()`并`gc.freeze()`，自动GC仍保持启用、阈值仍为`700/10/10`；gen2最大暂停下降但3K数量级问题仍在，不能把GC冻结冒充根因修复。
2. 3K期间Journal恰好发生268 MiB segment rotation，旧segment sealer继承owner CPU64，`seal_work_ns`达到约2.77秒并与热路径尾部重叠。`d0c0529`将sealer纳入第四个显式role CPU；首次错误使用CPU160，与CPU64属于同一物理核，既有SMT门禁在启动前正确拒绝。`7ff17af`改为CPU77并把frontend暂降为7个；实测3次rotation全部完成、最大seal约1.15秒、maintenance无失败，owner不再被sealer占核。该失败启动证据保留，不能计入通过。
3. `7ff17af`的1K回归通过；3K前两轮通过，第三轮只有最后`T_end-10 ms`释放槽的8笔Submit和7笔Snapshot窗后完成。逐笔证据显示发生器排队约3.4 ms，服务端剩余约6.5–9 ms；同期无gen2或seal重叠。`d7a4945`使用已属于TradeCore独占分区的空闲SMT sibling恢复第8个frontend，不扩占CPU80/176；第三轮窗后降至2笔Submit，但仍按原标准判FAIL，前两轮不得挑选冒充三轮通过。
4. CPU80/176的只读核验发现其正在被Docker、Kafka、ClickHouse、GitLab等普通任务使用；因此没有扩大`tradecore.slice`，没有迁移或重启其他项目。`47e6fb2`把8个frontend固定到8个独立物理核70–76、78，只让轻量acceptor线程使用CPU166（CPU70的SMT sibling）。owner64、projector65、cold66、segment sealer77、两个projection read67/68、WRITE broker69和READ broker79继续不共享物理核；整个候选仍严格位于既有`64-79,160-175`独占分区。
5. `47e6fb2`低压10秒实测Submit 5,000＋Snapshot 5,000全部窗内、零错误；scheduled P95为4.151/4.198 ms，P100为6.808/7.291 ms，没有观察到相对前一候选的低压退化。随后不可挑选地连续执行三轮3K总负载，每轮Submit 15,000＋Snapshot 15,000均全部窗内完成、零窗后、零业务/传输错误：Submit P95为9.085/9.511/8.659 ms，P99为15.827/18.776/10.068 ms；Snapshot P95为8.073/8.430/7.774 ms，P99为10.125/9.855/9.077 ms。P100保留29.802/38.732/18.170 ms与17.056/14.369/14.385 ms，不能用P95通过隐去单笔长尾。
6. 三轮后候选实际完成3次rotation和3个sealed segment，`maximum_seal_ns=1,113,199,851`、`maintenance_failed=false`、queue/capacity dropped均0、writer未失败；GC启用且stable graph frozen，最大观察暂停10.436 ms。`MemoryPeak=3,528,073,216`字节，低于6 GiB，TasksCurrent=65。实际PID/TID亲和性核对了owner64、sealer77、8个frontend70–76/78、brokers69/79以及acceptor TID166；不是只检查命令行参数。

证据根为`/home/xuanling/tradecore-perf-47e6fb2/`，汇总`acceptance-summary.json` SHA-256=`a30dd42566f49b2f5b8f4699c612c6bc835771d10076395a07b2bc54266ff2a3`；1K及3K R1/R2/R3报告SHA-256依次为`1eec74847221c6b0c22241bdcd46ded44614d287f93f04af506424ef584ff5e4`、`0c4f718939d1905d25506e95d016b9cf2ca3f3d6d3ac483a4c19acb1855498c0`、`074367e29c9a5199e25c2a35675f52cb19335caef622b9022a61685434714119`、`2f3e07e79c224427060052ef6572167d4b696daf7dfd8b8cc966ea8f164c0956`。三个正常服务全程active/running、Result=success、NRestarts=0；候选仍是独立transient unit，没有替换正常生产wheel或公共socket。

**下一门禁。** 先在该精确固定拓扑运行60秒HIGH并保留完整outcome ledger、segment/GC/资源同期证据；通过后再补LOW、HOT、背景公平、突发→降压及L3–L6长期runtime组合。短窗通过只解除“进入下一阶段”的阻塞，不是十二小时启动证书。

#### 2026-10-26 E5b-7：60秒HIGH内存软边界

状态：**4.5 GiB主动回收边界连续三轮通过，可推广到正式Consumer资源合同；60秒HIGH性能只通过1/3，仍不得标记完整HIGH或长期门禁通过。** `09d4514`在候选systemd cgroup增加`MemoryHigh=4,831,838,208`字节，保留`MemoryMax=6,442,450,944`字节；运行时准入仍按扣除有界、干净inactive file cache后的不可回收占用判断，不把`memory.events max`、OOM或OOM-kill降级。三轮均使用全新进程、generation和Journal目录，发生器固定在非交易核，同时释放2,000 Submit TPS与2,000 Snapshot QPS共60秒，并运行公开PollEvents/FULL checkpoint消费者。

1. R3中Submit 120,000与Snapshot 120,000全部在窗口内完成，零业务/传输错误；scheduled P50/P95/P99/P100分别为Submit 5.333/13.623/45.777/68.747 ms、Snapshot 4.176/7.836/12.993/44.499 ms，严格性能门禁通过。
2. R4的240,000请求全部成功，但4个Submit和5个Snapshot在窗口后完成；R5全部成功，但5个Submit在窗口后完成。因此R4/R5仍按既定窗口标准失败，不能以最终守恒替代时限，也不能挑选R3冒充连续三轮通过。
3. 三轮cgroup峰值分别为4,832,669,696、4,832,620,544和4,832,817,152字节；`memory.events high`分别增加1,266、1,264和1,265，证明内核确实在软边界执行回收/节流。三轮`max/oom/oom_kill`增量均为0，候选`NRestarts=0`、`Result=success`，故内存安全子门禁连续三轮通过。
4. 资源采样均取得105/105周期；宿主未提供`cgroup.io.stat`和per-task block-delay计数，报告保持`complete=false/NOT_MEASURED`，不把缺失字段写成0。原始证据位于`/home/xuanling/tradecore-high60-09d4514-r3-run`至`r5-run`；两次发生器参数编排失败的R1/R2也保留，且没有计作产品轮次。

**推广边界。** 本阶段仅证明软内存边界能阻止文件页缓存顶到6 GiB硬上限；它不证明HIGH连续三轮通过。正式Consumer可采用相同`MemoryHigh/MemoryMax`合同并做重启、socket、cgroup和只读健康验收；后续仍须定位窗口尾部4–9个请求的调度抖动，再以独立namespace重跑连续三轮。

#### 2026-10-26 E5b-8：broker同turn fan-in减排

状态：**代码实现与本地验收阶段；服务器同口径结果未产生前不得标记性能通过。** R4/R5逐笔Consumer evidence与owner IPC trace按`request_id_sha256`关联后，越界Submit的客户端`sent→owner received`为8.019–9.318 ms，而owner接收后排队仅0.027–0.032 ms、业务处理0.172–0.287 ms、owner回包后到客户端0.087–0.176 ms。高延时位于frontend/WRITE broker到owner之前，不应继续修改OMS Journal、mmap、FULL投影或内存上限。

固定发生器每10 ms共同释放请求；八个frontend在同一轮可能让单通道出现第4或第5个已ready请求。原broker完成一次阻塞select后只允许2次零超时即时poll，连同首轮最多推进3次，剩余已ready包会跨到下一selector turn。修改把这一硬上限从2提高到4：每次仍为`select(0)`，空结果立即停止，不等待、不忙轮询，不增加128共享总credit、4个READ slot、输出包预算或任何队列。WRITE/READ均保持既有协议、顺序、unknown/fence和故障关闭语义。

验收顺序：先跑broker协议/故障/多frontend本地与Linux回归；再以全新候选依次运行LOW/HOT与60秒2K Submit＋2K Snapshot三轮。每轮必须全部守恒、零业务/传输错误、`memory.events max/oom/oom_kill=0`，且所有计划请求在共同窗口内完成；只改善P50/P95或最终排空不能替代连续三轮严格通过。

首个Linux候选的LOW轮中，5,000 Submit和5,000 Snapshot均全部窗内完成且无数据面错误，但独立PollEvents在600个事件后以`PUBLIC_RPC_OR_CHECKPOINT_FAILED`停止，服务同期记录read-owner `BrokenPipeError`，所以整轮明确失败。根因修改本来只由WRITE的`sent→owner received`证据支持，不应同时扩大READ调度；后续修订为WRITE最多4次即时poll，READ保持原2次，重新从Linux回归和全新候选开始，不复用失败进程或该轮checkpoint。

修订revision `60cff13`本地33 passed/3个平台skip，Ruff/Pyright无错误；Linux补齐私有owner evidence根后36 passed、0 failed/skipped，JUnit SHA-256=`2ea54612b565e31a40ca7021b0ab3e3aea26ea39eec33884158e524c88cf9258`。全新候选LOW中5,000 Submit与5,000 Snapshot全部窗内、零错误；scheduled P50/P95/P99/P100分别为Submit 1.969/3.445/3.918/31.982 ms、Snapshot 2.204/3.269/3.650/31.719 ms，PollEvents子进程正常结束，证明READ回退消除了上一轮回归。

随后串行HOT执行10,000预热＋10,000正式Submit，全部`ACCEPTED`且LSN唯一，P50/P95/P99/P100为0.627/1.128/2.647/11.799 ms；旧HOT门禁要求P95≤0.30 ms、P99≤0.50 ms，因此仍严格失败，未进入HIGH。10,000正式样本与owner IPC trace全部关联：调用开始→owner收到P50/P95/P99为0.292/0.449/1.755 ms，owner排队0.030/0.038/0.046 ms，owner业务0.179/0.296/0.718 ms，owner回包→客户端结束0.091/0.118/0.138 ms。仅owner业务P95已接近0.30 ms，再叠加fresh-per-request连接及多进程handoff后，当前完整路径不可能通过原单进程级0.30 ms门禁。

**当前需要发布标准决策。** 保留P95≤0.30/P99≤0.50 ms意味着必须另做低并发专用架构（例如受控持久连接及更少IPC跳数），不能靠继续增加即时poll达成；若固定多frontend隔离架构是长运行定档，则应依据其真实基线另行批准HOT阈值，同时保持HIGH、故障和资源标准不变。未取得该决策前，`60cff13`只保留为未推广的候选revision，正式wheel不安装，HIGH与十二小时门禁不继续。

#### 2026-10-26 E5b-9：HOT 三轮独立基线与阈值定档

状态：**三轮基线完整，固定多 frontend、fresh-per-request 的 HOT 合同重新定档为 P95≤1.50 ms、P99≤3.00 ms；HIGH、故障、资源及 LOW 标准均未降低。** 三轮均基于`d91daca5b7f2f7649091cd40c16d8a25b56119c6`，每轮创建全新候选进程、generation 与 Journal namespace，执行10,000预热＋10,000正式单在途Submit。三轮全部10,000/10,000 `ACCEPTED`、LSN全唯一，无请求、协议或Journal错误。

| 轮次 | P50 / P95 / P99 / P99.9 / P100（ms） | completed TPS | cgroup峰值 | memory.events异常增量 |
|---|---:|---:|---:|---:|
| R1 | 0.631 / 1.171 / 2.523 / 3.477 / 12.342 | 1,396.97 | 2,151,104,512 B | 0 |
| R2 | 0.629 / 1.186 / 2.450 / 3.105 / 12.094 | 1,399.89 | 2,150,420,480 B | 0 |
| R3 | 0.633 / 1.207 / 2.582 / 3.425 / 11.655 | 1,390.27 | 2,151,632,896 B | 0 |

新阈值以最差完整轮次 P95=1.207455 ms、P99=2.581901 ms 为基线，增加明确运行余量并取整至1.50/3.00 ms；不以P100单个调度离群点放宽主体分位，但P99.9/P100仍为强制证据。summary SHA-256分别为`3d0eaee07f8526328a5f06f45a78365af013377390d86bc258ab28325978f7e1`、`86aafb903f2e60aaeadc899fc91d5761209f8a726c5d3cffd2a9b6e882e4ed59`、`d870daa90030687351ea30dbe2e7dc1c2933526c509947c0baf82f54d6650519`；原始样本SHA-256分别为`11a2fc13af6b071eab6956ed545f5e8c14f57945b21d8e2471a140606d069b10`、`bb49ccac761478a0f50f5a09443c200e74e539993c21890bc2b512f51ee23ac8`、`6fd2b200393cd71da9e1a43eecd0dee08cf7255d7ffb20e7733d3e00eba7dc69`。证据目录为服务器`/home/xuanling/tradecore-hot-baseline-d91daca-r{1,2,3}`及对应`-server-evidence`目录。

探针必须把阈值profile和数值写入summary；请求完整性与延迟门禁继续分开，只有两者同时通过才可`passed=true`。本次阈值只替代固定多frontend HOT的旧0.30/0.50 ms合同；LOW仍保持原0.30/0.50 ms标准，HIGH仍要求共同窗口内全部完成、零错误，故障与资源标准原样保留。

定档提交`6a7feb9`推送并由服务器快进后，以第四个全新候选完成独立资格轮Q1：10,000预热＋10,000正式样本全部`ACCEPTED`且LSN唯一，P50/P95/P99/P99.9/P100为0.625/1.169/2.442/3.196/11.056 ms，`requests_passed=true`、`latency_slo_passed=true`、总`passed=true`，探针退出码0。summary和samples SHA-256分别为`459185de4686cc0ddb8dbef4cfeb01a477805915ddde2114d90fa0bd3c7b0813`、`079a7f6ced4d3292b39f5342357520dfc684be19a4e016f7fc9689b906e2892b`，证据位于服务器`/home/xuanling/tradecore-hot-qualification-6a7feb9-r1`。候选随后停止，正式Consumer恢复；TradeCore、fault supervisor、Consumer三个正式服务均为active，Consumer `Result=success`、`NRestarts=0`，保持MemoryHigh 4.5 GiB、MemoryMax 6 GiB。HOT门禁据此完成，可进入保持原标准的HIGH验收；这不等于HIGH、十二小时、Adapter或交易READY已通过。

#### 2026-10-26 E5b-10：原标准60秒HIGH三轮复验

状态：**原HIGH标准未通过；两组各三轮都只有1/3通过，保持HIGH `NOT ACCEPTED`，不得进入十二小时、S8或交易`READY`。** 标准没有调整：每轮以全新进程、generation和Journal namespace，同时运行2,000 Submit TPS与2,000 Snapshot QPS共60秒；两类各120,000笔必须全部在共同窗口内完成，业务/传输错误和窗后完成均必须为0。公开PollEvents还须在测量窗后完整drain到本轮已知终点，且离线source、projector和最终状态必须闭合。最终drain目标只作为停止条件，不冒充source authority。

`2ededbb`先补齐common-window对最终drain参数的传递及报告字段；本地相关测试通过。基于该revision的首组三轮结果为：R1 Submit 119,984窗内、16窗后，R2/R3两类各120,000全部窗内。三轮240,800条consumer事件均最终checkpoint，source/projector最终LSN均为481,600，Mock均retire 120,400单且`native_calls=0`。三轮只有R2/R3性能通过，故首组严格失败。R1的16条越界请求与owner IPC trace按请求摘要16/16关联：`release→call` P50/P95为0.618/1.046 ms，`call→sent`为0.537/0.691 ms，`sent→owner received`为16.214/18.002 ms，owner排队为0.028/0.056 ms；主要等待明确位于frontend到WRITE broker/owner之前，而不是Journal或owner业务。

`dde00ea`据此只把固定8 frontend下WRITE broker的零等待即时poll上限扩大为8，READ保持2；每次仍为`select(0)`、空结果即停，不改变128共享credit、4个权威READ slot、FULL、单writer、队列容量、顺序、unknown/fence或关闭语义。本地相关36 passed、2个平台skip；Linux broker/owner组合38 passed，JUnit位于`/home/xuanling/dde00ea-broker-linux.xml`，SHA-256=`65b17d8858de11bbf2882c27b46c9dac61774b3b6cf16b6d06bbc1ce36658740`。

优化后LOW第一轮的数据面5,000 Submit＋5,000 Snapshot全部窗内，但PollEvents第57次RPC在4,043个事件后超时，报告`PUBLIC_RPC_OR_CHECKPOINT_FAILED`，因此该轮失败；第二个全新候选的数据面仍全部窗内，最终drain到10,800事件并通过。HOT回归10,000预热＋10,000正式单全部`ACCEPTED`且LSN唯一，P50/P95/P99/P99.9/P100为0.641/1.178/2.539/3.465/10.654 ms，符合已批准的1.50/3.00 ms门禁。LOW的一次失败不能由后一次通过覆盖，说明继续无差别提高poll预算存在读侧公平风险。

优化后的原标准HIGH三轮如下，所有分位均为共同释放计划时间到响应完成的`scheduled`累计延迟：

| 轮次 | Submit窗内/窗后 | Snapshot窗内/窗后 | Submit P50/P95/P99/P100（ms） | Snapshot P50/P95/P99/P100（ms） | 窗内/最终checkpoint | 结果 |
|---|---:|---:|---:|---:|---:|---:|
| R1 | 120,000 / 0 | 120,000 / 0 | 5.362 / 14.189 / 50.954 / 71.303 | 4.110 / 7.959 / 12.919 / 42.533 | 230,985 / 240,800 | PASS |
| R2 | 119,997 / 3 | 120,000 / 0 | 5.394 / 16.671 / 47.710 / 87.622 | 4.110 / 7.995 / 13.033 / 77.070 | 213,034 / 240,800 | FAIL |
| R3 | 119,992 / 8 | 120,000 / 0 | 6.267 / 25.838 / 54.362 / 99.549 | 4.350 / 8.684 / 13.287 / 54.591 | 174,350 / 240,800 | FAIL |

三轮均零业务/传输错误，最终drain分别补齐9,815、27,766和66,450条事件；每轮source写入240,800条、source/projector最终LSN=481,600、Mock retire=120,400、`native_calls=0`、7次rotation/7个sealed segment、`maintenance_failed=false`。cgroup峰值分别为4,833,062,912、4,832,739,328和4,832,100,352字节；`memory.events high`分别增加1,266、1,513和123，`max/oom/oom_kill`均为0。资源采样每轮105/105，但宿主仍缺少`cgroup.io.stat`及per-task block-delay accounting，所以这些字段保持`NOT_MEASURED`，不得写成0。transient multifrontend helper没有生成统一的全子进程lifecycle证书；systemd成功退出和source闭合不能替代该缺口。

原始证据位于服务器`/home/xuanling/tradecore-high60-dde00ea-r{1,2,3}-run`及`/var/lib/tradecore-performance-candidates/20261026T052837Z-dde00eab8915`、`20261026T053052Z-dde00eab8915`、`20261026T053308Z-dde00eab8915`。三轮load报告SHA-256依次为`a7d98353bb5653bb3696c2aa30f4b6d94d8716527d5f7939a1ee2fab29f3bd80`、`a926ed7762b3319cbbfc84f64a68f9bee400847b1abbef3dcf2d96aaaf8dc1ff`、`9680864980e2c99b1f240457b1a37a1aff465e7406799ea32382e68b0ea0dd66`；events报告SHA-256依次为`c80076e9c0e373592ca8ddacfd78eb3b565f6cdc838f109e8486e50841e2788d`、`f0aab88cacb88969100cb5395565b6b8fd0bce610d55b638f2b9cc1a07c8a120`、`f785ff46ab5b5ff8b836b3aec74a36b105160574e182455d7a1f6799fcc8b39c`。

候选已停止，正式TradeCore、fault supervisor和Consumer UDS三项服务均恢复为`active`；Consumer `MainPID=3728772`、`NRestarts=0`、`Result=success`、`MemoryHigh=4.5 GiB`、`MemoryMax=6 GiB`。服务器checkout包含候选源码，但没有用本轮候选替换正式已安装wheel。

**下一优化边界。** 不再继续单纯增加即时poll次数，也不改Journal/mmap/FULL或放宽60秒窗口。下一包应把WRITE fan-in改成“每turn时间预算＋每frontend公平轮转”的有界调度，并显式预留READ服务预算；先连续三轮LOW、HOT和故障/关闭回归，再以全新namespace重跑HIGH三轮。任何优化都必须消除R2/R3的3/8条跨窗，同时不能重复本轮LOW的PollEvents超时，才能重新标记HIGH通过。

#### 2026-10-26 E5b-11：有界时间 turn、公平轮转与 READ 预留验收

状态：**代码、LOW、HOT、故障关闭回归和正式 wheel 部署均已完成；本阶段通过，但不替代尚未执行的新一轮原标准 60 秒 HIGH 三轮。** 最终运行 revision 为 `14c1354d24375268d817412fcb916dfb43701904`。本轮没有调整共享 128 总 credit、Journal/FULL、单 writer、队列容量、6 GiB 硬上限、HIGH 窗口或故障关闭标准，也没有启动 EMT/QMT Adapter。

1. `b4949e2`取消“固定即时 poll 次数代表工作量”的做法：WRITE broker 每个已唤醒 turn 最多运行 1 ms，READ broker最多运行 0.5 ms；第一次阻塞 `select` 不计入预算，后续只允许 `select(0)`，无 ready event 立即结束，既不 sleep 也不 busy-spin。selector 返回的 frontend event 按 frontend 分桶交错，每个连续 ready frontend 每轮先贡献一个事件，起始 frontend 在 turn 间轮转；输出 flush 同样轮转，WRITE 每 frontend 最多推进现有 STARTED＋terminal 两个有界包，READ 每通道每轮只做一次非阻塞发送。owner event 仍优先推进，协议顺序、unknown/fence、CRC、共享 credit 与关闭语义未改变。
2. READ 不是依赖“恰好空闲”的软公平：8 个既有权威 rendezvous 中，snapshot 最多占 6 个，物理保留 2 个给 replay/control；replay 本身仍只允许 1 个在途。`0abc8b2`至`8d0fd6c`增加跨进程 READ demand 信号和仅在 replay/control 等待时生效的投影锁短让渡，正常 snapshot 热路径不固定 sleep。`1ec2bf0`和`14c1354`把 broker、owner READ/WRITE 与 request reactor 的空闲唤醒上限由 50 ms 收紧至 5 ms；这是阻塞 idle select 上限，不是热 turn 的等待。
3. 被否决的试验保留为反证：READ 线程 GIL interval 1 ms 没有稳定收益，已由 `67f5d6a`回退；扩大 snapshot 或无差别增加 READ poll 会重现 PollEvents 超时，未进入最终版本；投影让渡只在共享需求信号有效时使用，未把 mmap、Redis 或同步 I/O重新放回热路径。

LOW 连续三轮均以全新候选、10 秒、500 Submit TPS＋500 Snapshot QPS执行。每轮两类各 5,000 请求全部窗内完成、零业务/传输错误、最终事件排空闭合，所有 LOW 检查均为 true：

| 轮次 | Submit P50/P95/P99/P100（ms） | Snapshot P50/P95/P99/P100（ms） | load / events SHA-256 |
|---|---:|---:|---|
| R1 | 2.0193 / 3.4591 / 3.9567 / 31.3098 | 2.1975 / 3.4140 / 8.0937 / 31.3590 | `3f576750dc7a3583317aa79286205c0b3370a840f3609aba174eabd91dde52c0` / `37f4763697a85d48fe4fdb61b642515b02c8a7a3b1ddfcc087bf3e806e1a4d7e` |
| R2 | 2.0429 / 3.5103 / 4.1144 / 29.4748 | 2.2200 / 3.4120 / 8.0855 / 29.7861 | `6ceaf99a4124e6a0db8ca3dfc92e46e52365a1d9793092409635ab4bfe70080d` / `36123dfd126abe32713058aac9e26810787757b4aa291f93b45e88fbc68ae402` |
| R3 | 2.0470 / 3.5123 / 4.0714 / 36.9951 | 2.1996 / 3.3874 / 8.1142 / 31.3426 | `8fe841f2e6f1bb7894b5d4a006387a3f9445f24f90589ba283e51634ea3f36a5` / `8798ae7009e9aa017f85f246eaae82bba0e0ba05434b228254a24a19042a94d7` |

HOT 连续三轮均为全新候选、10,000 笔预热＋10,000 笔正式单在途 fresh-request；每轮 10,000/10,000 `ACCEPTED`且 LSN 唯一，均通过已批准的 P95≤1.50 ms、P99≤3.00 ms：

| 轮次 | P50/P95/P99/P99.9/P100（ms） | completed TPS | summary SHA-256 |
|---|---:|---:|---|
| R1 | 0.622791 / 1.162596 / 2.484845 / 3.401536 / 10.499508 | 1,421.80 | `c0e79e5988e018c843b0f1c2555cfe6bd9370ec4c012c52a6e0849c17136fce0` |
| R2 | 0.625675 / 1.186310 / 2.468537 / 3.155795 / 10.968064 | 1,412.65 | `7ee01dff2dfa42231aa7ac37eb8dab12c284ca55fb114517a36544d0466ffcf7` |
| R3 | 0.631473 / 1.153007 / 2.536375 / 3.197953 / 11.808115 | 1,404.48 | `dc076297f6d8d9430e8e4bced4771f2be1dc1a62b11d556daa847ee9ff45e999` |

故障/关闭资格轮在 Linux 实际执行 24 passed（79.10 秒），覆盖 broker/owner/frontend 故障、未知 WRITE、READ 丢失、受控 drain 与关闭证书；systemd 结果 success。JUnit 位于 `/home/xuanling/14c1354-functional-fault-qualified-r1.xml`，SHA-256=`ada9d3a049c926c5c953afafa72e80b623411a58ca4375a70d042c6b013e3ce2`。最终 Linux 定向回归为 103 passed、6 个明确平台/资格条件 skip，JUnit `/home/xuanling/14c1354-owner-wake-linux.xml`，SHA-256=`69ed52e31a632f01f2f39a6a0e2fda51618e7cb5970c693b3b8d258a003ff2fc`；skip 不属于本轮故障路径。

正式部署使用服务器 Git checkout 精确 `14c1354`和 CPython 3.10 wheel，wheel SHA-256=`60cbdd345599497dacd4bd9afadf16e445651075cef45c4a088a345bdd26cd2d`。旧安装包、entrypoint、unit/drop-in、配置、部署前后服务状态和校验和均保留在 root 私有目录 `/var/lib/tradecore-deployments/20261026-turn-fair-14c1354`。部署脚本首次恢复时错误先启动 fault-supervisor；其 mount namespace 因 runtime 尚未创建 `/run/xuanling-tradecore`而被 systemd fail-closed，失败日志完整保留。修正启动次序为 runtime→fault-supervisor→Consumer UDS 后，三个正式服务均 enabled、active/running、`Result=success`、`NRestarts=0`；新 PID 分别为 856932、856975、856984。Consumer 保持 `AllowedCPUs=64-79,160-175`、`MemoryHigh=4.5 GiB`、`MemoryMax=6 GiB`，启动后 `memory.events high/max/oom/oom_kill`均为 0，CPU partition 复核通过；socket 仍为 `xuanling:tradecore-testers:0660`，正式只读 `ListAccounts`返回`COMPLETED`，performance candidate 为 inactive。安装后的变更运行文件逐项与 checkout 哈希一致，两个正式服务用户均从 `/tmp`导入 site-packages 成功。

**严格边界：** LOW、HOT及故障关闭已经满足本阶段先决顺序，正式 wheel 已部署且在线健康通过；但这次正式普通 Consumer unit 本身不是多 frontend 性能候选入口，不能把只读冒烟写成候选拓扑性能证明。更重要的是，`14c1354`尚未运行新的原标准60秒HIGH连续三轮，因此HIGH仍为`NOT ACCEPTED`，S8、十二小时、Adapter及交易`READY`继续关闭。下一步必须用三个全新namespace执行2,000 Submit TPS＋2,000 Snapshot QPS各60秒，并要求每轮120,000＋120,000全部在共同窗口内完成、零错误、checkpoint及资源闭合。

#### 2026-10-26 E5b-12：公平 turn 后的原标准 60 秒 HIGH 连续三轮

状态：**Submit＋Snapshot 的 60 秒 HIGH 请求窗口门禁连续三轮通过；完整 HIGH 仍未通过，原因是 checkpoint P95 和统一 lifecycle certificate 两项证据门禁未满足。** 测试 checkout 为只增加文档的 `b3e0e026f4a74169c9c8c9ca1c6cef3d407a7fed`，运行代码仍为已部署的 `14c1354d24375268d817412fcb916dfb43701904`。每轮均创建全新 candidate、generation、Journal namespace 和业务序列空间，同时释放2,000 Submit TPS与2,000 Snapshot QPS共60秒；每类精确120,000笔。Adapter/EMT/QMT均未启动，`native_calls=0`。

第一次编排在T0前暴露项目运行venv缺少性能工具依赖`hdrh`，load/events两个子进程同时以`ModuleNotFoundError`退出；candidate的submitted/written均为0，没有业务请求、Journal写入或性能样本。该目录`/home/xuanling/tradecore-high60-fairturn-b3e0e02-r1`永久标记为编排无效，不计入三轮，也未被删除。随后从现有uv离线缓存把`hdrhistogram==0.10.8`及其依赖安装到独立的`/home/xuanling/.local/share/tradecore-performance/python310-b3e0e02`，没有修改正式wheel或正式服务依赖；CPython 3.10对`hdrh`及两个发生器入口完成导入预检后，才开始连续计数R1。

| 轮次 | Submit窗内/窗后 | Snapshot窗内/窗后 | Submit scheduled P50/P95/P99/P100（ms） | Snapshot scheduled P50/P95/P99/P100（ms） | checkpoint窗内/最终 | 请求门禁 |
|---|---:|---:|---:|---:|---:|---:|
| R1 | 120,000 / 0 | 120,000 / 0 | 4.825 / 11.698 / 51.118 / 79.233 | 3.848 / 8.331 / 9.945 / 60.981 | 223,504 / 240,800 | PASS |
| R2 | 120,000 / 0 | 120,000 / 0 | 4.887 / 11.592 / 47.383 / 73.335 | 3.871 / 8.339 / 9.970 / 41.812 | 223,488 / 240,800 | PASS |
| R3 | 120,000 / 0 | 120,000 / 0 | 4.915 / 12.493 / 51.970 / 73.728 | 3.844 / 8.286 / 9.904 / 51.446 | 221,126 / 240,800 | PASS |

三轮均满足Submit scheduled P95≤20 ms/P99≤60 ms、Snapshot scheduled P95≤10 ms/P99≤40 ms，且两类generator queue P99均约1.22～1.24 ms、低于10 ms。所有计划请求均窗内完成，业务/传输错误、unissued、overload和容量丢弃均为0。每轮测量后公开PollEvents均drain至240,800个唯一事件；离线source/checkpoint逐事件join均匹配240,800条、240,800个source batch。候选受控停止后，三轮Mock均retire 120,400单、source/projector最终LSN均为481,600、7次rotation/7个sealed segment、`maintenance_failed=false`、source timing dropped=0。

资源安全连续三轮通过：`memory.events high`增量为1,265/1,263/1,264，证明4.5 GiB软边界实际工作；峰值分别为4,832,698,368、4,832,870,400、4,832,612,352字节，低于6 GiB；`max/oom/oom_kill/oom_group_kill`增量全部为0。每轮资源采样105/105且无采样错误；宿主仍不提供`cgroup.io.stat`和per-task block-delay accounting，明确保持`NOT_MEASURED`，不写成0。

完整HIGH不能据请求窗口结果提前签发。三轮`PollEvents.call_to_progress` P95/P99分别为34.374/37.061、34.308/37.192、34.111/37.159 ms；P99满足40 ms，但P95均高于定档10 ms。离线join虽然逐条匹配全部事件，因multi-frontend helper仍未生成统一的post-runtime lifecycle certificate，`producer_closure_status`只能为`NOT_EVALUATED`；独立读取最终status可以证明本轮Mock/source/projector闭合，却不能替代版本化签署证书。因此汇总结论固定为`request_high_three_round_passed=true`、`full_high_three_round_passed=false`，不得启动十二小时或声明S8/交易`READY`。

证据目录为服务器`/home/xuanling/tradecore-high60-fairturn-b3e0e02-r{1,2,3}-run`，对应candidate根为`/var/lib/tradecore-performance-candidates/20261026T083536Z-b3e0e026f4a7`、`20261026T083845Z-b3e0e026f4a7`、`20261026T084156Z-b3e0e026f4a7`。三轮load报告SHA-256依次为`7860efd308d2abf4f58992361b1623485545a49be1ca959e82fd8e4004b6635e`、`5f91f993aea2421e89b69e2a9e64621f0c12a71d1963bbfb5e7efc5199337973`、`af8b8b34f4865d1086f07d4debd01b36a53786e718d032c12db0f193b986fef6`；events报告依次为`0a6ca89d3357ecc9ab1bb3906076ff7d9e0c8d54eb6cc0742475a35949eded05`、`65e57459c2463e9d60808f9803b20d00c6d4949e196037d9289950d91d95d784`、`76d2d72833ff9cf30427823002c0d313a0f87f879729063626fc4336ed45dc49`。总汇总`/home/xuanling/tradecore-high60-fairturn-b3e0e02-summary.json` SHA-256=`b213e8ee256b5791f0630757e3bf38e209aeaee9d2d15b215f9d28db1800a970`。收尾后performance candidate为inactive/dead；三个正式服务均active/running、`Result=success`、`NRestarts=0`。

**下一优化边界：** 不再修改Submit/Snapshot主请求调度或放宽窗口。下一阶段只处理两项剩余证据：把事件checkpoint从当前约34 ms P95压至≤10 ms且保持P99≤40 ms；为multi-frontend candidate生成覆盖acceptor、所有frontend、READ/WRITE broker、OMS owner、read plane及owned helper的统一、hash绑定、受控关闭lifecycle certificate。两项均完成故障回归后再重跑完整HIGH，不用本次请求子门禁的PASS替代。

#### 2026-10-26 E5b-13：checkpoint 调度、统一 lifecycle certificate 与原标准 HIGH 复验

状态：**checkpoint 容量正确性和统一 lifecycle certificate 已完成；原标准 60 秒 HIGH 连续三轮仍为 `NOT ACCEPTED`。** 最终计数三轮统一运行在 revision `9ce66fbe79da691f319a616eb8fa5ba4e6d222e1`。每轮均使用全新进程、generation、Journal namespace 和业务序列空间，同时运行 2,000 Submit TPS＋2,000 Snapshot QPS 共 60 秒；每类计划请求 120,000 笔，Adapter/EMT/QMT 均未启动。原门禁未放宽：请求必须全部在共同窗口内完成；`PollEvents.call_to_progress` P95≤10 ms、P99≤40 ms；最终须 drain 240,800 个唯一事件至权威 Journal LSN 481,600，并完成 source/status/lifecycle 三方闭合。

本阶段落地以下修复：

1. `d748fd8`、`3c466ab`把 FULL checkpoint 的 WAL 调度收口为有界周期 checkpoint。旧实现使用 `wal_checkpoint(RESTART)` 后按物理 WAL 文件长度执行 64 MiB 门禁，但 SQLite RESTART 只复用 WAL、不缩短物理文件，第一次跨阈值就会错误 fail-closed。最终改为周期 `wal_checkpoint(TRUNCATE)`，仍保持 FULL、单 writer、逐条 checkpoint 和故障关闭语义；三轮各完成 20 次成功 truncate，均完整 checkpoint 240,800 条事件，没有再次中断。
2. `0d19652`、`af68fee`、`c6f5339`和`ace1b89`补齐 generation 不可变、hash 绑定的统一 lifecycle certificate，并把合法拓扑上限修正为生产实际的 13 个角色：8 个 frontend、acceptor、WRITE broker、READ broker、OMS owner 和 read plane。三轮均取得 `MATCHED_FRESH_MOCK_SOURCE`，逐条匹配 240,800/240,800，关闭 120,400 个 Mock 订单；systemd 退出成功不再被当作证书替代物。
3. 4 replay partition 的试验使 LOW 的 checkpoint P95/P99 退化至约 47.3/64.6 ms，已由 `e8e4df0`恢复单 partition。额外 replay queue weight 和 50 ms idle poll 也没有改善低负载 checkpoint，分别由 `761bc35`、`9ce66fb`回退；这些反证不进入最终运行配置。

最终连续三轮结果如下。请求分位均为共同计划释放时刻至响应完成的 `scheduled` 累计延迟；checkpoint 分位为逐次公开 `PollEvents` 从调用到取得进展的配对时间：

| 轮次 | Submit窗内/窗后 | Snapshot窗内/窗后 | Submit P50/P95/P99/P100（ms） | Snapshot P50/P95/P99/P100（ms） | checkpoint P50/P95/P99/P100（ms） | 最终事件/LSN | 严格结果 |
|---|---:|---:|---:|---:|---:|---:|---:|
| R1 | 120,000 / 0 | 120,000 / 0 | 4.940 / 11.633 / 42.729 / 64.225 | 3.887 / 8.344 / 9.937 / 42.402 | 6.754 / 24.838 / 26.624 / 60.031 | 240,800 / 481,600 | FAIL |
| R2 | 120,000 / 0 | 119,999 / 1 | 4.973 / 13.427 / 52.298 / 78.119 | 3.901 / 8.315 / 9.986 / 50.201 | 6.754 / 24.658 / 26.345 / 45.974 | 240,800 / 481,600 | FAIL |
| R3 | 120,000 / 0 | 120,000 / 0 | 4.927 / 12.567 / 49.414 / 72.614 | 3.883 / 8.266 / 9.970 / 44.532 | 6.754 / 24.658 / 26.477 / 39.813 | 240,800 / 481,600 | FAIL |

三轮业务/传输错误均为 0，最终事件、source、projector、状态和 13 角色 lifecycle certificate 均闭合。候选峰值内存分别为 4,832,808,960、4,832,800,768、4,832,776,192 字节；`memory.events high`增量分别为 1,546、1,578、1,578，`max/oom/oom_kill/oom_group_kill`增量全部为 0。R1/R2/R3 的 checkpoint WAL truncate 均为 20 次且无失败。

严格失败有且仅有两类：R2 有 1 个 Snapshot 在共同 60 秒窗口之后完成，破坏“连续三轮每类 120,000 全部窗内”的请求门禁；三轮 checkpoint P95 均约 24.66～24.84 ms，高于原定 10 ms，尽管 P99 均低于 40 ms。分段证据显示瓶颈主要在服务端 `PollEvents.call` P95 约 20.91～21.04 ms，本地 FULL checkpoint 增量约 3.7～4 ms；继续调整 WAL、增加 partition、提高 queue weight 或延长 idle poll 都不能解决该数量级问题。

证据位于服务器 `/home/xuanling/tradecore-high60-final-9ce66fb-r{1,2,3}`，候选根分别为 `/var/lib/tradecore-performance-candidates/20261026T100405Z-9ce66fbe79da`、`20261026T100650Z-9ce66fbe79da`、`20261026T100940Z-9ce66fbe79da`。三轮 load SHA-256 依次为 `b73d2168ae1d5621594d53fc6a9f768452b04768ec72bc69ec4407cdc1f56de9`、`3ae68fe3c387ced6a69ff72298b9205acdbea75b74b4586a6ccbaf35e06c523d`、`77f84ab672bee354c4888d43e9c616ddfb7d245348fdb126a4296964077f2a14`；events SHA-256 依次为 `8b56f0cc4afd677a6d4482cfecd012574bd8d045754b55e482790c4cfd9b7d83`、`19862c2783d4e27371498e126b283f58606d145ae78a38d469fdef50381cce0f`、`170ba4d48dd302061456dc921bc34fd8d0364e15b92f6c10bb289ff04cc68a4c`；join SHA-256 依次为 `33bba7fa09b67a12d3b24346f36b3b6731a45298c10912e297e4d095bd5cd5d6`、`6c8bebc353bfa585659d2130d99f9b284c975388340186876e3be63ec0b2d468`、`df8e4d6503d2d65565b568ffec1884857dfd95f0a76d5240f5e1395cecc16f32`。

候选已停止；正式 `xuanling-tradecore.service`、`xuanling-tradecore-fault-supervisor.service`、`xuanling-tradecore-consumer-uds.service` 均为 active，Consumer `MainPID=1699820`、`Result=success`、`NRestarts=0`。由于最终净运行时改动只涉及性能 checkpoint/join 工具，正式 wheel 未被无意义替换，服务器 checkout 已同步最终 revision。

**下一架构边界：** 若保持原 checkpoint P95≤10 ms，不应再修改 Journal/FULL、重复增加 replay partition 或依靠 GIL/轮询参数碰运气；应把 replay READ 缩短为独立低跳数服务路径或专用 read plane，并单独消除 60 秒窗口最后一个 slot 的调度抖动。完成 LOW、HOT、故障关闭回归后，必须再用三个全新 namespace 重跑完整 HIGH；在此之前十二小时、S8、Adapter 和交易 `READY`继续关闭。

#### 2026-10-27 E5b-14：专用低跳数 replay READ 与窗口尾 slot 收口

状态：**代码、三轮 LOW、三轮 HOT、受控关闭证书和最后 slot 窗口证据已通过；完整 60 秒 HIGH 尚未在本 revision 重跑，因此 HIGH、十二小时、S8 和交易 `READY` 仍不升级。** 最终运行 revision 为 `cfe80c89da9dec285a4235b1f3ae399227a5695b`。

实现不再将“去掉 READ broker”误写成 replay 已隔离。`0af2c08`只完成 frontend 到 owner 的直连；同期 LOW 实测 `PollEvents.call_to_progress` P95 仍为 25.936 ms，证明瓶颈不在已移除的 broker。`815ff36`和`cfe80c8`随后将每个 frontend 的权威 READ 物理拆成两条固定通道：

- `PollEvents/CreateReplayPartitions` 只能进入 replay rendezvous，直达 OMS owner 的有界 committed replay window，不再经 snapshot read-plane、SQLite 子进程及其 GIL/调度队列；
- snapshot/other READ 仍进入独立 read-plane，不把大快照编码负载搬回 OMS owner；
- frontend 与 owner 两端都校验 lane，错路由 fail-closed；总 credit=128、总 READ credit=8、snapshot 最多6、Journal/FULL、单writer、帧协议、授权及交付前再校验均未放宽。

`b290acb`将100 Hz开环释放时钟确定性布置在`T0`至`T1-60 ms`，不减少总请求、不改观测窗口、不用窗后drain冒充窗内完成。`815ff36`同时修复多 shard 报告把单 shard `125 TPS`误写成整轮速率的问题；三轮最终报告均为`planned_each=5000`、`window_average_tps=500`、`last_release=T1-60 ms`，且 Submit/Snapshot 各5000条全部在`T1`前响应。

LOW 三轮均使用全新进程、generation、Journal namespace和业务序列空间，10秒内同时运行500 Submit TPS＋500 Snapshot QPS；每轮10,000个公开请求零业务/传输错误，10,800个唯一事件全部 FULL checkpoint，final drain达到目标：

| 轮次 | Submit scheduled P50/P95/P99/P100（ms） | Snapshot scheduled P50/P95/P99/P100（ms） | PollEvents call P50/P95/P99/P100（ms） | call-to-progress P50/P95/P99/P100（ms） |
|---|---:|---:|---:|---:|
| R1 | 2.259 / 3.609 / 4.440 / 35.717 | 1.839 / 2.875 / 3.140 / 31.588 | 0.731 / 1.199 / 2.171 / 11.149 | 4.006 / 4.809 / 6.300 / 22.692 |
| R2 | 2.234 / 3.604 / 4.313 / 32.719 | 1.831 / 2.886 / 3.154 / 32.604 | 0.713 / 1.081 / 2.114 / 15.262 | 3.963 / 4.755 / 6.181 / 19.612 |
| R3 | 2.234 / 3.647 / 4.342 / 37.323 | 1.828 / 2.906 / 3.160 / 31.048 | 0.729 / 1.114 / 2.037 / 10.527 | 3.990 / 4.805 / 6.418 / 19.497 |

与`b290acb` LOW R1的`PollEvents.call` P95=20.906 ms、`call_to_progress` P95=25.936 ms相比，最终三轮 P95 稳定为1.081～1.199 ms和4.755～4.809 ms；后者低于原定10 ms，P99最差6.418 ms低于40 ms。证据目录为服务器`/home/xuanling/tradecore-low-hotwindow-cfe80c8-r{1,2,3}`；load/events SHA-256依次为`bdff5cae...0008f2/60251b99...528e`、`3c0c7a3a...62df1/46ee514f...f1e0`、`4abab1de...bf0af/37b4beb5...b249`。

三轮受控停止均为 systemd `Result=success`，都生成12角色、`runtime_closed=true`的hash绑定lifecycle certificate，SHA-256依次为`a59630c9a25e5ff8f815a99282b370fdba31157474d45fc8260acc8be3a7d99b`、`5f5e2fa38df7452160e785962bc4b8d2d0d6357c94deedc8a1907b54da97d0e7`、`9c879cf99c2601da616be6e64e53f6c597735a21d06e8fcc4ec79836b5fcbbf8`。直连拓扑没有 READ broker，因此12而非旧13角色是实际完整集。

HOT三轮各10,000预热＋10,000正式单在途 fresh-request，都为10,000/10,000 `ACCEPTED`、LSN唯一，P50/P95/P99/P99.9/P100依次为`0.621/1.165/2.553/3.380/21.621 ms`、`0.628/1.174/2.473/3.291/19.528 ms`、`0.630/1.173/2.484/3.335/25.872 ms`，均通过 P95≤1.50 ms、P99≤3.00 ms的已定档门槛。summary SHA-256依次为`dcfec1a353b15efa57ee4c459732ba137fb32d96722f4de1f28a2558684ea483`、`3c2d7eb6f27b3606525e500812e0fb214a1574b7d88f083d1ad3e2f5b19e3bba`、`14723e559a7b6375fe2d04cd5f66888867f9ff43a23813fb594b35e358477737`。

两个失败证据不被删除或改写：首个`815ff36`集成候选把`allowed_lanes`误接到submission worker，在T0前便退出，修正为`cfe80c8`后重建全新 namespace，不将该轮计入性能样本。另外对独立read-plane实施`SIGKILL`时，全部frontend收到EOF并拒绝交付，没有伪造成功响应；该轮属破坏性容器故障，最后由systemd有限`TimeoutStopSec=75`清场，不具备clean lifecycle，不得写成受控关闭通过。正常受控关闭证据以上述三轮LOW为准。

候选已停止，服务器checkout为`cfe80c8`。正式`xuanling-tradecore.service`、`xuanling-tradecore-fault-supervisor.service`、`xuanling-tradecore-consumer-uds.service`已按runtime→supervisor→Consumer顺序恢复，三者均active、`Result=success`、`NRestarts=0`；公式`ListAccounts`只读冒烟返回response。

**下一步严格边界：** 本阶段已消除原25 ms replay READ P95和窗口尾 slot口径问题，但不用LOW代替HIGH。下一节必须在`cfe80c8`后续revision上用三个全新namespace重跑原60秒HIGH，要求每轮120,000 Submit＋120,000 Snapshot全部窗内、零错误、checkpoint P95≤10 ms/P99≤40 ms、source/status/lifecycle闭合，才能重新判定完整HIGH。

#### 2026-10-27 E5b-15：低跳数 Journal replay READ 与 60 秒 HIGH 最终验收

状态：**专用 replay READ、窗口最后 slot、LOW/HOT、独立读平面故障关闭以及原标准 60 秒 HIGH 三轮均已通过。** 性能验收 revision 为 `576e6555ecb8e08bdb3c989911600804a419884c`；验收后仅补充冷启动 fence 保活、原始失败证据传播和单 frontend 双平面兼容，当前代码 revision 为 `44b25882d5eae1c07b3fca087b15b8a41409d799`。后续三项不进入生产多 frontend 稳态请求热路径，不用新 revision 冒充重跑过的性能样本。

最终实现不再让 replay 在 OMS writer、SQLite projection 或 snapshot worker 之间多跳：

- 16 条只读 rendezvous 物理拆成 8 条 replay 与 8 条 snapshot/other；分别由 `owner-replay-plane` 和 `owner-snapshot-plane` 两个独立进程树持有，各自拥有 coordinator、SQL fallback 与固定 CPU；
- replay plane 从 owner 发布的 committed Journal fence 起步，后台连续校验 mmap frame、CRC、commit word、batch 连续性和 recovery seed，生成有界 committed replay window；公开请求只读取已投影窗口，窗口缺口才 fail-safe 回退权威 SQL 路径；
- pump 每轮冻结一个固定 committed prefix，生产者持续提交不能让一次读取追逐移动尾部；每个 batch 只在短临界区更新有界 cache，`PollEvents` 不执行 Journal 扫描、SQL 查询或 writer IPC；
- `ORDER_ATTRIBUTED + RESERVATION_CREATED` 走严格 seed/intent/hash/identity 校验，状态变化仍走规范 reducer；缺少 predecessor、校验失败、读平面退出或 fence 过期全部围栏，不猜测订单状态；
- `b290acb`把开环计划最后一次释放固定在 `T1-60 ms`，保持请求总数和共同窗口不变，消除了因浮点/循环尾项落在 `T1` 附近造成的最后 slot 抖动。

曾经失败的方案继续保留为反证：双进程但 replay 仍经 SQLite 时 LOW P95 约 34.7～37.9 ms；同步 Journal refresh 虽使 LOW/HOT 通过，但 HIGH 首次请求会追逐移动尾部；固定 prefix 同步读取在 10 秒 HIGH 中将 `PollEvents`放大到约0.4～0.98秒。最终采用“后台固定前缀 pump＋公开请求只读已发布窗口”，而不是提高队列权重或放宽 FULL。

最终 60 秒 HIGH 使用三个全新 candidate、generation、Journal namespace，持续同时输入 2,000 Submit TPS＋2,000 Snapshot QPS。每轮计划与完成均为 Submit 120,000/120,000、Snapshot 120,000/120,000，全部在共同窗口内完成，业务错误、传输错误和未发行请求均为0：

| 轮次 | Submit scheduled P50/P95/P99/P100（ms） | Snapshot scheduled P50/P95/P99/P100（ms） | PollEvents call-to-progress P50/P95/P99/P100（ms） | 窗内/窗后事件 | 最终唯一事件/LSN | 结果 |
|---|---:|---:|---:|---:|---:|---:|
| R1 | 4.858 / 11.715 / 44.827 / 67.043 | 2.787 / 4.624 / 7.565 / 38.109 | 4.151 / 6.578 / 8.520 / 190.972 | 175,849 / 64,951 | 240,800 / 481,600 | PASS |
| R2 | 4.841 / 12.567 / 51.413 / 74.383 | 2.822 / 4.600 / 7.549 / 32.997 | 4.137 / 6.713 / 8.438 / 209.060 | 177,647 / 63,153 | 240,800 / 481,600 | PASS |
| R3 | 4.891 / 11.985 / 45.023 / 67.043 | 2.785 / 4.616 / 7.500 / 35.357 | 4.164 / 6.492 / 8.479 / 187.695 | 176,514 / 64,286 | 240,800 / 481,600 | PASS |

`PollEvents` P95 6.492～6.713 ms、P99 8.438～8.520 ms，均通过原定 P95≤10 ms、P99≤40 ms；Submit 通过 P95≤20 ms、P99≤60 ms，Snapshot 通过 P95≤10 ms、P99≤40 ms。P100保持报告项，不用少量调度极值推翻已定分位门禁。三轮最后 release 均在共同窗口结束前，且每轮两类各120,000请求全部窗内，故“最后一个 slot”不再依靠窗后 drain 伪装通过。

三轮证据目录依次为服务器 `/home/xuanling/tradecore-high60-pump-576e655-r{1,2,3}`；candidate 根分别为 `/var/lib/tradecore-performance-candidates/20261026T164946Z-576e6555ecb8`、`20261026T165215Z-576e6555ecb8`、`20261026T165444Z-576e6555ecb8`。load SHA-256 依次为 `c076037e968551572c9dbec910b09c8fe8466bea309a873911385312f4e776c7`、`ba34f34160a70412be92a11d35fb0bde008d63d55fb26cac14533a65ab754f08`、`1b4c3a7973f9e7e9b9d7052ef43085061c3011b5353821dfd4c63a26043ba324`；events SHA-256 依次为 `63e52cdd9e5a511229e0d2a135412e8d9aa27e3d913c12f2058b87af3832eaa8`、`34e3f4a67d710c6f7537b539d690760e57a6ef55862e1c8c65fd2528b970b806`、`b40203bbadcaa36fa29a6038f96a853a4751267296d026456dca6b555ec48339`。

三轮均生成13角色的完整 lifecycle certificate，包含独立 `owner-replay-plane` 与 `owner-snapshot-plane`，`runtime_closed=true`、`all_owned_descendants_exited_zero=true`、helper `remaining_descendants=0`；证书 SHA-256 依次为 `3b7ef3773bcfeaa793f8c2a12ee0f357aa83f07aa0fac76510859de7d7e8ff69`、`1ae9222c3ba39cf042b7dc176fc1e068fdde04bb92143e559ae08fdb7a0af5bf`、`6e0a0f484d8cc417311d2c250b1032445f0e1aa1d3d72ae977e7a96027812460`。R3 最终 projector/source 都到达 LSN 481,600，source submitted=written=240,800、dropped=0、failed=false、closed=true；cgroup `memory.max=6 GiB`，`memory.current=4,828,225,536`字节，其中可回收 file 1,207,816,192字节，`memory.events max/oom/oom_kill`均为0。

当前 revision 的专项回归：本地 Ruff、Pyright 均通过，相关测试 `28 passed, 18 skipped`；Linux 独立 systemd/cgroup 真实进程测试同时覆盖“读平面 SIGKILL 后全局围栏且不重启”和“Submit、replay、snapshot、audit 合同”，结果 `2 passed in 14.60s`、unit `Result=success`。回归同时发现并修复两个启动边界：双读平面构建期间提前持续发布 fence，避免首平面命中2秒 stale gate；单 frontend 回退也严格按前8条 replay、后8条 snapshot/other 路由，不再套用旧8通道合同。

候选已停止，服务器 checkout 为 `44b2588`。正式 `xuanling-tradecore.service`、`xuanling-tradecore-fault-supervisor.service`、`xuanling-tradecore-consumer-uds.service` 均为 active/running、`Result=success`、`NRestarts=0`；正式 UDS `ListAccounts` 冒烟返回 `COMPLETED`且1个账户。至此本节 HIGH 阻塞解除；十二小时、S8、Adapter 及交易 `READY` 仍是独立后续门禁，不能由本节性能通过自动升级。

#### 2026-10-27 E5b-16：清理后恢复原60秒短轮三轮压测

当前执行范围：用户明确不再要求十二小时压测，只执行既有短轮。最新三轮严格HIGH结论为 **0/3通过，`FAILED_STRICT_HIGH`**；吞吐、请求完整性与低延迟验收分开报告，不沿用E5b-15的历史通过结论替代本次结果。

执行前修复离线`join_consumer_source_timing`的旧单read-plane拓扑合同：允许runtime实际发布的单读平面或replay＋snapshot双平面，并按实际拓扑检查read-broker是否存在；持续拒绝缺失/混合读平面、重复进程与非零退出。该变更仅影响离线证据校验，未修改业务热路径。Ruff通过，本地31 passed（36.56秒）、Linux31 passed（36.02秒），代码提交`bbb6d817f72f0b8dea7109ce2cfa7fd559e5cedc`已push并通过服务器Git拉取。

2026-10-27北京时间约10:06至10:13，依次创建三个全新进程、generation和Journal namespace。每轮共同60秒窗口内同时2,000 Submit TPS＋2,000热页QPS，四个发生器进程使用`88/184`、`89/185`、`90/186`、`91/187`，独立事件消费者使用`92,93,188,189`，观测与运维使用`94,190`。服务仍固定`64-79,160-175`、MemoryHigh=4.5GiB、MemoryMax=6GiB；入口credit、FULL、256 batch/512 fact及1,024 seed缓存均未放宽。每轮额外400笔seed只计准备数据，不补测量吞吐。

三轮两种操作都各计划120,000、实际发行120,000、成功120,000、窗内完成120,000；未发、窗后完成、业务错误、传输错误均为0。合计720,000次测量请求，实际每轮Submit TPS=2,000、热页QPS=2,000。以下scheduled延迟从逐笔计划释放至客户端响应校验完成，单位ms：

| 轮次 | Submit P50/P95/P99/P100 | 热页 P50/P95/P99/P100 | PollEvents call→FULL progress P50/P95/P99/P100 | 最终checkpoint唯一事件 | 严格结果 |
|---|---:|---:|---:|---:|---:|
| R1 | 6.091 / 23.577 / 51.347 / 205.652 | 3.090 / 5.681 / 8.221 / 51.479 | 9.134 / 12.886 / 20.595 / 137.101（失败前535个成功页） | 19,958 / 240,800 | FAIL |
| R2 | 5.898 / 32.784 / 61.702 / 163.840 | 3.115 / 5.349 / 8.090 / 81.396 | 4.366 / 6.992 / 9.126 / 148.767 | 240,800 / 240,800 | FAIL |
| R3 | 5.718 / 26.526 / 56.132 / 77.922 | 3.080 / 5.243 / 7.926 / 38.404 | 4.334 / 6.971 / 8.897 / 206.438 | 240,800 / 240,800 | FAIL |

Submit P95三轮均超过原20ms门槛，R2 P99还超过60ms；热页三轮通过原P95≤10ms/P99≤40ms。R2/R3调用级checkpoint通过原P95≤10ms/P99≤40ms，但R1第669条消费者progress记录是`PollEvents`返回`CURSOR_RECOVERY_UNAVAILABLE`，随后停止消费；仍有220,842个本轮源事件未checkpoint。R1失败前分位只描述已完成页，不代表完整总体，也没有用重试或补跑替换本轮。

三轮source/projector最终都到LSN481,600，source submitted=written=240,800、dropped=0、failed=false；Mock退休120,400笔、filled=96,320、rejected=24,080、native_calls=0。三轮都生成13角色完整lifecycle，runtime_closed=true、all_owned_descendants_exited_zero=true、helper remaining_descendants=0。R1正常关闭并不弥补事件缺口，其完整source/FULL关联退出1，保持失败证据；R2/R3离线逐条匹配240,800个事件，均为`MATCHED_FRESH_MOCK_SOURCE`。

调用级checkpoint时间不能冒充事件生产到消费的累计时间。R2/R3分别有164,013/163,373个事件在共同窗口内checkpoint，其余76,787/77,427个在窗后drain约26.94/27.49秒后完成。包含准备与最终drain的全量source→FULL checkpoint配对上界P95分别25.837/26.340秒、P99分别26.827/27.364秒；这些覆盖完整本轮事件，不能用约7ms的单次RPC/checkpoint P95掩盖积压。

每轮130个1秒资源样本完整写出，采样内存峰值依次4,831,739,904、4,831,580,160、4,831,621,120字节；`memory.events max/oom/oom_kill`增量和`cpu.stat throttled_usec`增量均为0。资源报告仍`complete=false`：每轮130次采样均缺少`cgroup.io.stat`和启用的per-task block-I/O delay accounting，故不能宣称完整I/O归因证据通过，也不能据此将Submit长尾归因为磁盘或CPU不足。此次未修改主机全局观测开关、IRQ、其他项目资源或热路径。

证据根为服务器`/home/xuanling/tradecore-short-20261027T020605Z-bbb6d81`，每轮`r{1,2,3}/window`保留逐笔raw/HDR、consumer checkpoint和progress，`resources`保留资源采样，R2/R3的`source-join`保留完整配对。candidate根依次为`/var/lib/tradecore-performance-candidates/20261027T020606Z-bbb6d817f72f`、`20261027T020824Z-bbb6d817f72f`、`20261027T021049Z-bbb6d817f72f`。`acceptance-summary.json` SHA-256=`eafb3db5b9f8bbdcd1f901c7faf8ab11c38ca2b9eed8b16503ca5e5b24516833`，`operator.json` SHA-256=`c39012571e41dda80a519bfcfc67c3dbc42df1466f9d422d05e37047fdf2d4c8`。

三轮load报告SHA-256依次为`2a4147218bab3a95b81de4136284dcf898c99617f14a07a0a2387c16b6c7a25c`、`7a6de6b99b77f3f0218e794377ad8dba447cb35d3b15ad70081eb146e766ca13`、`48569f73c400c8eccd353b2463dc447190d0b73cfd05b5f6a3a0fd1a4f94eda1`；lifecycle SHA-256依次为`5a22e2c1619a7f1dbc644ed245fa5fb3b8d0e1f2bc8584eafb24874443c6e5d7`、`3462b6763526c3d28e66d90faff573e5933840189d0ecec17bc362c7b820134d`、`b7739e6052638e9b8f27c91f0181adfa6c326c45e1fd08dbe058370ea4f7c216`。R2/R3完整source join报告SHA-256分别为`5f9bf0ea89cba9cb68274b75b5f3cb7efd80d2fe50511f6927ed2ad08480461e`、`19e0976cb7c52d058d848f84fc3a02e0b952c6f6c7a71fbeee53968e2054170d`。

候选与发生器均已停止，正式runtime、fault-supervisor、Consumer三个服务已恢复active。当前需要单独解决Submit尾延迟及R1的cursor恢复错误，再重新验收；不把吞吐达标升级为严格HIGH整体通过。本轮没有启动十二小时压测，没有交易Adapter副作用，也没有进一步修改业务运行代码。

#### E5b-17：回放安全边界与 Submit 尾延迟诊断（进行中）

新增确定性回归证明：当缓存直接签发尚未由 SQL 投影提交的范围、随后范围被缓存淘汰时，持久投影回退无法满足签名范围，可能产生 `CURSOR_RECOVERY_UNAVAILABLE`。修复将热缓存可签发上界限制在已通过健康/代际检查的投影已提交水位与 Journal 已提交水位的较小值；被淘汰的已签发前缀仍从持久投影读取，签名、作用域和故障关闭不放宽。投影落后时返回已证明前缀，不把空结果解释为源端不存在新事件。该确定性风险与旧 R1 错误相符，但旧 R1 没有足够的内部错误现场，不能断言已证明它是唯一根因；需经新三轮持续验证。

本地相关回归 31 passed，Ruff 通过。根据旧 R1 120,400 条 owner IPC 的独立汇总，owner 排队 P50/P95/P99/P100 为 0.0285/0.0408/0.0550/1.2146 ms，owner 内执行为 0.2130/0.4728/3.5361/56.7699 ms；测量覆盖 seed，尚未与客户端逐笔 scheduled 延迟关联，不能将其直接视为同一总体分位或将其归因于磁盘/CPU。后续需完成分段采样、同条件 A/B 以及原标准 60 秒三轮；在此之前严格 HIGH 和 P0 线上验收均为未完成。

隔离诊断使用每前端有界脱敏 trace，逐请求连接 20,400 条前端/owner 记录；Submit 慢段主要在前端 offer 至 owner 收到，而非 owner 内接纳。代码检查和单元测试发现，WRITE broker 在四条 offer 的本轮预算耗尽后，可因 level-triggered 可读 socket 反复空轮询直至 1ms 预算到期；修改为本轮 flush 后立即让出并重开 turn，保留四条上限、队列/credit上限、owner单线程与公平轮转。Ruff 通过，本地 30 passed/2 skipped，Linux相关回归29 passed。

同机同资源、fresh namespace、10秒各2,000 Submit TPS＋2,000热页QPS的有 trace 诊断 A/B：A=`7c02ee5`，B=`c6d569d`。两轮均 Submit/热页各20,000窗内成功，事件消费者40,800唯一事件并达到LSN81,600目标，不能因此替代离线source join。Submit scheduled P50/P95/P99/P100：A 5.439/17.646/50.266/71.565 ms，B 4.936/11.690/41.779/61.833 ms；20,400条逐请求配对的frontend offer→owner receive P50/P95/P99/P100：A 3.351/14.244/46.962/69.021 ms，B 2.977/8.995/39.259/59.744 ms。各为单轮，且trace存在扰动，仅作诊断对照，仍须无额外frontend trace的正式60秒三轮验证；用户已取消12小时压测。

随后以`6ff6b5d`关闭前端额外trace执行原标准三轮，证据`/home/xuanling/tradecore-high60-broker-20261027T052810Z/r{1,2,3}`，结论 **0/3通过**。R1约23,657 Submit完成后read-plane退出并连带隔离候选失败，83笔收到RATE_LIMITED，事件未完成；日志显示`owner replay Journal pump failed`，但旧版未记录底层异常类型，不能声称已确定该次根因。R2/R3分别各120,000 Submit与120,000热页窗内完成，240,800事件均通过source/checkpoint闭合，但Submit scheduled P95/P99分别29.426/65.700 ms、21.234/54.657 ms，仍未达到P95≤20ms、P99≤60ms；热页与调用级PollEvents P95/P99通过。R2/R3全量source→FULL checkpoint P95分别24.109/25.619秒，需区分逐页调用延迟与累计积压。

本次资源采集器启动参数错误地包含了不属于候选cgroup的发生器CPU，采集被边界检查拒绝，三轮资源证据缺失；不得声称资源门禁通过。该执行错误已记录，后续应只采集候选有效CPU并另行采集发生器。三轮候选停止后正式三个服务已恢复active。

另以确定性测试复现：已淘汰的前驱状态使可选回放缓存丧失覆盖时，旧实现把主动停掉的缓存线程误判为意外故障，进而围栏整个read-plane；已修改为回退权威持久投影，真实异常或仍声称覆盖时的线程意外退出仍fail-closed，并增加脱敏异常类别。此缺陷与R1日志表现一致，但R1旧日志未保留异常类别，不能断言其为本次唯一根因；修复后的独立回归和原标准重测仍是必要项。

`bf26eab`修复后再次在独立候选执行三轮60秒，每轮Submit和热页均120,000/120,000窗口内完成，事件各240,800条均与fresh mock source配对闭合，未复现cursor错误或read-plane退出；此为三轮证据，不等于证明旧R1的唯一根因。Submit scheduled P50/P95/P99/P100（ms）依次为5.530/17.564/48.431/96.076、5.616/29.213/62.980/127.402、5.665/26.935/55.509/90.636；热页P95/P99依次5.128/7.901、5.177/8.143、5.136/8.126 ms。PollEvents call→FULL progress P95/P99依次7.066/9.470、6.959/9.216、6.959/9.257 ms。严格Submit门限仅第一轮通过，因此性能三轮未通过。

各轮窗后分别还需checkpoint 73,197、73,743、73,189条；全量source→FULL checkpoint P95分别24.629、25.199、24.092秒，不用约7ms的单次调用统计替代积压事实。各轮资源130个采样，内存峰值分别4,831,723,520、4,831,379,456、4,831,703,040字节，采样的oom增量与throttled_usec增量均为0；仍缺cgroup.io.stat及逐任务block-I/O delay，完整资源门禁未通过。依据R2样本及lifecycle，OMS owner有多条线程同绑CPU 64，累计runqueue等待最高三条约14.605、13.098、12.984秒（覆盖130秒采样，不是单请求延迟），提示线程竞争值得进一步拆分，但尚不能单凭关联确定是Submit长尾唯一原因。证据`/home/xuanling/tradecore-high60-replay-20261027T053713Z/r{1,2,3}`；三个正式服务恢复active。

#### E5b-18：候选后台线程分离试验（待 A/B）

在现有TradeCore cgroup `64-79,160-175` 内，候选设置后台owner线程CPU `171,172`，WRITE actor显式继承CPU 64；不扩大cgroup、不排斥其他项目或更改宿主机IRQ。171/172分别与前端75/76共享物理核，**不是物理独占核**，只作为受控试验，必须测量前端/快照退化。Python线程仍共享GIL；核分离不等于并行执行Python。原非试验配置仍可省略`--background-cpus`，不修改其语义。候选保持单writer、原协议、credit和Journal安全边界不变；完整回归、低并发、A/B及三轮HIGH未完成前不宣称通过。

同revision `22ab6b9`、同资源的A/B结果：A基线10秒混合两类各20,000全部窗内完成，Submit P95/P99为46.039/69.665 ms，事件40,800条source闭合；A单笔HOT 10,000/10,000通过，P95/P99为1.322/2.644 ms。B后台分离10秒轮仅18,522笔Submit成功、318笔RATE_LIMITED，P95/P99为90.898/119.865 ms（仅成功样本），事件未闭合，候选在测试期间发生read-plane失败；B HOT在预检阶段连接被拒绝，并非一笔单测性能结果。日志已记录回放pump异常类`MmapOmsFactJournalRecoveryRequired`，具体原因仍未留证，不能仅据此认定是核竞争或证明数据损坏。B为失败试验，不进入默认运行配置；`create`和`create-baseline`恢复原亲和性，只有显式`create-experimental`使用该试验参数。上述证据在`/home/xuanling/tradecore-affinity-ab-20261027T060909Z/`，不能把试验假冒验收通过。追加白名单脱敏故障分类，用于后续安全定位，不输出原始异常文本。

试验再次复现读平面关闭，白名单定位为`OWNER_FENCE_UNAVAILABLE_OR_TORN`：原固定16次紧凑采样未取得一致快照。增加最多2ms、4096次、有调度让出的有界重试，只接受序列一致的完整快照，真正持续损坏、陈旧或不可用仍fail-closed。此修复不证明旧R1唯一根因，线程分离试验复测HOT P95/P99为1.769/3.127 ms、HIGH10发生302笔RATE_LIMITED及未完成，仍维持默认关闭。

基线逐笔脱敏诊断显示20,400次Submit actor墙钟累计4.247秒、线程CPU累计3.800秒；47次单次actor墙钟≥2ms，中位CPU仅0.220ms、非运行/阻塞约2.499ms。另一独立诊断中，55次actor墙钟≥2ms有47次与Mock终态批次时间重叠；关联不证明唯一因果。Mock每8笔让出试验10秒A/B：A P95/P99 26.198/58.687 ms，B 21.676/63.603 ms，B并未通过，默认关闭。每笔让出在短轮和三轮LOW/HOT中通过，但原标准60秒三轮P95/P99依次35.979/61.374、39.682/67.174、28.623/58.327 ms，**0/3通过**；每轮两类请求均120,000/120,000完成，240,800事件source闭合。这是no-Adapter Mock调度试验，不是实际EMT性能优化，不得按短轮结果启用。证据分别位于`/home/xuanling/tradecore-yield-ab-20261027T065231Z/`、`tradecore-yield1-gates-20261027T070304Z/`和`tradecore-yield1-high60-20261027T070856Z/`；三轮资源采集仍缺`cgroup.io.stat`和逐任务block-I/O delay，资源证据不完整。

仅降低候选Mock后台线程nice +3、无提高任何线程优先级的试验：三轮LOW/HOT与真实线程故障关闭回归通过；60秒三轮均完成120,000 Submit、120,000热页和240,800事件source闭合，但Submit P95/P99为17.793/50.758、14.320/38.306、23.790/112.067 ms，第三轮失败；第二轮PollEvents call→FULL progress P95=10.084ms，亦超过10ms原门限。该Mock-only配置保持默认关闭，不能声称真实EMT通过；证据`/home/xuanling/tradecore-nice-high60-20261027T072527Z/r{1,2,3}`。

三轮资源采样分别观察到`memory.events high`累计增量1,564/1,817/1,815，`max/oom/oom_kill`增量均0、CPU throttled增量0；这些表明4.5GiB memory.high被触发，不能据此单独认定它造成Submit长尾。cgroup io统计不可用的直接环境原因是`tradecore.slice/cgroup.subtree_control`未启用io，内核`kernel.task_delayacct=0`；为遵守不影响其他进程的范围，本轮未修改主机全局参数或启用控制器，因此完整资源门禁仍未通过。

候选源码通过Git在`/opt/xuanling-tradecore`更新；**未把未通过的试验方案安装到正式生产wheel**。正式Consumer unit从`.venv`已安装wheel运行，其`direct_url.json`显示构建来源`tradecore-build-14c1354`；三个正式服务恢复active，Consumer UDS只读`ListAccounts`返回`COMPLETED`。源码checkout更新、候选测试和正式wheel发布为不同状态，不能混称部署验收通过。

#### E5b-19：经授权的 I/O 与任务调度统计补测

用户授权在诊断窗口临时启用全机`kernel.task_delayacct`以及TradeCore父cgroup的`io`控制器，并在结束后恢复。原值分别为`0`及未启用；执行时设置独立的8分钟恢复计时器和退出清理。第一次候选启动因服务器旧版`/usr/local/sbin/tradecore-performance-candidate`仍配置2个projection read CPU，而现有源码要求4个CPU，启动前失败、未产生压测结果；全机及cgroup设置均已恢复。后续以服务器Git checkout中的新版候选启动器启动`create-baseline`，未替换正式服务或旧受限sudo入口。诊断结束后复核`task_delayacct=0`，`tradecore.slice/cgroup.subtree_control`恢复`cpuset cpu memory pids`，正式Consumer UDS保持active。

第二次以`8e523bd5aa35058c238619ba073b18b9b5062782`执行无Adapter独立候选的60秒诊断：2,000 Submit/s和2,000热页/s各120,000请求均在窗内成功，模拟来源240,800事件与checkpoint逐条闭合，native calls为0。Submit scheduled P50/P95/P99/P100为5.157/15.737/44.270/131.334 ms；热页为2.832/5.050/8.245/193.331 ms；PollEvents call→FULL progress P95/P99为10.314/16.269 ms，**P95超过10 ms，整轮未通过原HIGH门禁**。窗后补齐59,429事件，source→FULL checkpoint P95为18.472秒；不能用单次Poll调用延迟代替事件积压。此为一轮带额外统计的诊断，不是三轮验收，也不代表真实EMT性能。

资源采集130/130，`complete=true`、缺失项为空，原始采样SHA-256为`f7ec28f4953230998f9906013e9f1a8a4818c8e81c747b2e8a3e4372a8f3b683`。60秒窗口内采样区间的cgroup写入约7.857 GB、写I/O 183,538次，`memory.events high`增量1,490，`max/oom/oom_kill`及CPU throttled增量均为0。CPU 64上的owner线程在窗口采样区间内分别累计约13.049、11.752、11.898秒runqueue等待；其中前两条累计block-I/O delay分别15、4 tick，内核`USER_HZ=100`。其他CPU上亦观察到块I/O等待。线程等待为跨请求累计值，不是单笔延迟；stat的block-I/O delay也不能等同所有文件同步等待。1秒采样和线程关联不能单独证明具体Submit长尾由CPU、内存或I/O造成。现有证据支持优先调查owner同核调度竞争，仍须独立对照及逐请求关联后才能确定方案和发布。

证据目录：`/home/xuanling/tradecore-io-diag-20261027T075040Z`；candidate根：`/var/lib/tradecore-performance-candidates/20261027T075040Z-8e523bd5aa35`；`source-join`结果为`MATCHED_FRESH_MOCK_SOURCE`。原HIGH、资源、故障与HOT标准均未降低，仍需完成有效优化及连续三轮完整门禁。

#### E5b-20：owner后台物理核隔离试验

依据E5b-19的owner同核调度证据，在原cgroup容量内尝试将owner后台放到物理核75/76。第一版错误将frontend从8减为6，被direct READ“每个权威slot一个frontend”的运行时防护在T0前拒绝，未发压；保留失败证据`/home/xuanling/tradecore-core-isolation-ab-20261027T080315Z`。修订后保留8个frontend，两个frontend分别移到71/72的SMT兄弟167/168，未关闭权威slot验证；默认候选不变。

同revision `640b5aa`、相同容量的10秒无Adapter对照：基线和隔离试验各20,000 Submit及20,000热页窗内成功、消费各40,800条事件；尚未作source逐条闭合。Submit scheduled P50/P95/P99/P100（ms）：基线`5.095/14.901/41.386/66.945`，隔离`11.526/51.446/70.582/95.158`；热页P95/P99分别为`5.100/8.421`及`6.177/8.618`，PollEvents call→FULL progress P95分别为`9.609`及`6.582`。隔离试验Submit超过原P95/P99门限，压测工具据此返回失败；**不得采用或部署该试验**。线程物理分核不消除Python共享GIL，单轮不能确定全部退化原因。两侧证据`/home/xuanling/tradecore-core-isolation-ab-20261027T080809Z`，只作为诊断，不计正式HIGH验收。服务器保持现有正式wheel。

#### E5b-21：候选owner的GIL切换对照与逐请求阶段

multi-frontend候选新增显式、默认不启用的owner `--gil-switch-us`实验入口，沿用既有允许值和spawn owner校验；`create-gil1`仅将候选owner配置为1,000微秒，不修改正式服务、其他进程或全机配置。Linux关联测试37 passed、6项条件跳过，跳过项不得计为故障验收。

第一次仅完成基线短轮，20,000 Submit和热页均窗内成功，但Submit P95=29.737ms、热页P99=57.606ms超门槛，原编排停止于B前，证据`/home/xuanling/tradecore-gil1-ab-20261027T081550Z`，不伪造B结果。随后保留失败退出码并完成两侧独立fresh候选对照：同revision `ddd445e`、10秒各2,000 Submit TPS＋2,000热页QPS，两侧各20,000/20,000窗内成功、各40,800事件已消费但未进行source逐条闭合。Submit scheduled P50/P95/P99/P100（ms）：基线`4.702/10.805/36.438/58.163`，1ms配置`5.071/13.271/49.644/73.073`；热页P95/P99为`4.547/7.696`与`4.903/7.979`，PollEvents call→FULL progress P95为`6.787`与`7.152`。该单轮的1ms配置相对基线退化，保持默认关闭；短轮不替代3轮HIGH或低并发/故障/资源门禁，不能断言其他负载下一定退化。证据`/home/xuanling/tradecore-gil1-ab-20261027T081731Z`。

从E5b-19相同60秒窗口的120,000条owner记录进行逐请求内部阶段统计：owner receive→finish P50/P95/P99为`0.2323/0.5675/3.1157 ms`；其中52条达到10ms，接纳段在这些慢请求中位数为10.8144ms，均未记录cold lookup。该内部总体不等同消费者scheduled总体，也不能用分位数相减得出排队时间或直接排除I/O/GIL。既有端到端配对诊断提示应继续研究frontend offer→owner receive的排队；下一次变更须保留逐笔配对、隔离变量、故障安全及原门限。当前仍未达到正式部署条件。

#### E5b-22：frontend→broker→owner逐笔诊断及有界窗口试验

默认关闭的候选诊断增加broker收到frontend帧、broker向owner offer、最后一次成功send syscall前后时间；以脱敏request hash、generation和owner central serial与frontend、owner trace关联。最后send返回与owner在另一CPU接收可并发，采用send前/后区间，不将其当成持久化或执行时间；缺失、重复、跨时钟、超界或writer丢失均不作为完整证据。`analyze_broker_handoff.py`用于对已关闭trace复算，不能把诊断测量当作正式资格。

`27b776f`的10秒诊断中，frontend、broker、owner各20,400条Submit完整且无重复，窗口内20,000条逐笔配对；各writer关闭、无丢失。frontend offer→broker收到 P50/P95/P99=`0.183/0.731/1.005 ms`，broker收到→owner offer=`0.0168/0.0333/0.0443 ms`，最后send完成→owner收到下界=`2.715/9.358/45.908 ms`，frontend offer→owner收到=`3.064/9.769/46.962 ms`。672条总区间≥20ms，其broker在途数中位77、最大126；这表明样本中主要等待位于broker发包完成后的单owner消费之前，但不证明每笔等待的唯一CPU或I/O根因。证据`/home/xuanling/tradecore-handoff-trace-20261027T083209Z`和candidate `/var/lib/tradecore-performance-candidates/20261027T083210Z-27b776f00438`。

试验保持原128总credit和原Journal，只在显式`create-handoff32`候选将owner handoff在途窗口设为32，并在窗口满时暂停已饱和frontend读取；信用仍须terminal交付后释放，关闭/异常仍fail-closed。Linux相关测试38 passed，本地36 passed、2 skipped。`5875271`同revision、无额外trace的10秒A/B各20,000 Submit和热页全部窗内成功、各消费40,800模拟事件但未source逐条闭合：Submit scheduled P95/P99基线`12.050/44.073 ms`、32窗口`19.431/46.039 ms`，热页P95/P99`4.510/7.561 ms`对`5.718/24.510 ms`；不支持作为优化。证据`/home/xuanling/tradecore-handoff32-ab-20261027T083636Z`。

另一次带trace的32窗口中，三侧各20,400条完整、窗口20,000条配对：最后send完成→owner收到 P95/P99=`10.702/22.005 ms`，但frontend offer→broker收到升至`3.715/35.577 ms`，frontend offer→owner收到为`17.771/50.967 ms`。936条总区间≥20ms的broker在途中位/最大均31；说明该试验把相当一部分等待移到上游，并未证明端到端改善。该轮Submit分位门槛失败，trace本身具有开销，不能将不同轮分位数相减作逐笔结论。证据`/home/xuanling/tradecore-handoff32-trace-20261027T083926Z`。32窗口与饱和时暂停读取仅在显式实验配置启用，默认仍128及原selector行为；禁止部署为性能优化。后续应减少单owner消费成本或探索保持单writer与恢复语义的执行隔离，再按既定LOW/HOT/故障/HIGH标准验证。

#### E5b-23：模拟终态维护批次试验

曾观察到部分owner慢段与Mock终态批次重叠，但关联不证明因果。为检验该假设，候选在保持所有终态事实、Journal、严格归档收据和retire校验不变时，显式比较Mock终态维护批次128与32；默认仍128，禁止将该无Adapter模拟器结果等同真实EMT。

同revision `a0e53be`、无额外frontend/broker trace的10秒2,000 Submit TPS＋2,000热页QPS A/B，两侧均20,000 Submit及20,000热页在窗内成功，各40,800事件消费到目标但未做source逐条闭合：Submit scheduled P50/P95/P99/P100（ms）基线`4.854/11.534/43.450/63.144`，Mock32 `4.817/21.381/63.046/101.122`；热页P95/P99为`4.579/7.778`及`4.903/8.290`；PollEvents call→FULL progress P95为`6.869`及`6.742`。Mock32的Submit超过原P95≤20ms、P99≤60ms，诊断轮失败、保持默认关闭；不能把单轮结论外推到真实柜台或其他负载。证据`/home/xuanling/tradecore-mock32-ab-20261027T084730Z`。此后仍缺可证明正向收益的修改、同条件多轮A/B及既定LOW/HOT/故障/HIGH资格，不得正式部署本次候选。

#### E5b-24：单owner计算减负与原标准复测

单owner线程的no-Adapter CPU诊断显示每5,400次请求调用`require_text` 550,800次，其中OMS fact属性字典的字段名存在确定的重复校验。候选仅在显式启用`TRADECORE_DIAGNOSTIC_KNOWN_FACT_KEYS=1`时，对13个固定、已验证的事实字段名免除重复文本检查；只接受精确builtin `str`，所有动态字段值、未知字段、子类、输入、恢复payload、权限、顺序、单writer、幂等和Journal校验保持原语义，默认关闭且不缓存账户或委托数据。无效值、未知/过长字段、字符串子类和错误消息的对照及Journal/恢复回归Linux 93 passed。本地关联测试81 passed；独立systemd有限CPU/6GiB、KillMode=mixed的实际提交后进程死亡、未知回执、伪造身份、读平面故障和fence回归16 passed；证据`/home/xuanling/tradecore-fact-keys-fault-20261027T091837Z`。跳过项不计通过。

同revision `6aca2c2`、两侧5,400请求的单owner线程CPU profile，仅用于定位而非性能资格：`require_text`调用550,800→469,800，`freeze_text_mapping`累计线程CPU约0.364→0.220秒，线程总CPU约4.539→4.384秒；有profiling扰动，单组约3.4%的CPU差异不能当作无扰动生产收益。无trace的2,000＋2,000短轮：首组基线Submit P95/P99=`14.713/53.379 ms`、候选`25.838/50.299 ms`；随后交错`B1,A1,A2,B2`分别为`11.756/44.007`、`24.084/48.136`、`11.600/39.485`、`10.838/18.448 ms`，每轮两类请求各20,000窗内成功、40,800模拟事件到达目标但短轮未逐条source闭合。必须保留首组失败及基线失败，不能只挑达标轮；证据`/home/xuanling/tradecore-fact-keys-ab-20261027T090315Z`、`/home/xuanling/tradecore-fact-profile-ab-20261027T090628Z`、`/home/xuanling/tradecore-fact-keys-interleaved-20261027T090842Z`。

候选HOT三轮各10,000预热＋10,000单在途样本均唯一成功，P95/P99分别`1.274/2.408`、`1.303/2.501`、`1.155/2.502 ms`，均满足P95≤1.50/P99≤3.00ms。LOW三轮各5,000 Submit及热页窗内完成，PollEvents call→FULL progress P95/P99分别`6.869/9.110`、`6.775/8.552`、`6.709/8.372 ms`，三轮各10,800事件与fresh mock source逐条闭合。证据`/home/xuanling/tradecore-fact-keys-hot-20261027T091324Z`及`/home/xuanling/tradecore-fact-keys-low-20261027T091500Z`。

在LOW、HOT、故障后按原60秒2,000 Submit TPS＋2,000热页QPS执行三个独立候选，**0/3通过**。Submit scheduled P50/P95/P99/P100（ms）为`5.456/32.293/65.110/128.516`、`5.517/52.953/119.669/428.081`、`5.259/21.823/61.571/138.150`；热页P95/P99为`6.099/30.638`、`7.172/42.861`、`5.014/8.262 ms`；PollEvents call→FULL progress P95/P99为`11.854/22.168`、`12.575/25.428`、`10.830/20.742 ms`。原Submit P95≤20/P99≤60ms、热页P95≤10/P99≤40ms、PollEvents P95≤10/P99≤40ms均未调整。每轮两类均最终120,000成功，R2的85笔Submit在窗口后完成而不是窗口内；不得用最终成功替代窗口门槛。

三轮各240,800模拟事件与fresh mock source/lifecycle逐条闭合，source→FULL全部事件上界P95分别约19.092/19.126/20.166秒，窗内消费179,900/178,842/177,677条；该源到checkpoint累计指标不能与单次Poll RPC混淆，包含种子和窗口外drain。三轮各130/130资源样本、`complete=true`、无缺失；窗口memory.high增量1,103/1,088/1,163，max/oom/oom_kill与CPU throttled增量为0。用户授权的`task_delayacct`和父cgroup io控制器均已恢复原值。HIGH发生器使用明确CPU亲和性，但报告仍明确不以此证明核独占，故即使未来数值达标也需完整资格证据，不能将本轮标为formal qualification。原始目录`/home/xuanling/tradecore-fact-keys-high60-20261027T092320Z`。

结论：已验证安全等价的候选计算减负，但没有证明Submit长尾稳定；13个固定字段名快速分支保持默认关闭。未安装生产wheel，正式服务保持原版本；后续需继续定位HIGH全局波动与队列/事件调度瓶颈，不能扩大或修改原门槛来宣布通过。
