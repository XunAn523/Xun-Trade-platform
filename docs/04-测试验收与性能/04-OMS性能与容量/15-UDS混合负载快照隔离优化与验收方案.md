# TradeCore UDS 混合负载快照隔离优化与验收方案

日期：2026-10-08。适用范围：Linux、项目 `.venv` CPython 3.10、Consumer V2 真实 UDS、无 Adapter 的 OMS 合成事件压测。

当前状态：**S5-A 至 S5-E 已实现并完成服务器部署与三轮正式混合验收**。正式性能服务端功能 revision 为 `07da49b`，最终合同/压测判定 revision 为 `845dbdf`；生产部署的 socket-ready 门禁 revision 为 `b6470a7`。2026-10-09 的 20 万订单准备阶段回归已由 `13f4e8c` 修复，reactor 突发接纳优化由 `9063722` 落地；当前部署以第 13.4 节的增量回归结论为准。本文同时保留原失败基线、实施约束、最终口径和真实证据，不将无 Adapter 的 OMS 合成验收扩展为 EMT/QMT 交易 `READY`。

目标是消除快照请求对 SubmitSignal 和 PollEvents 的秒级阻塞，建立有界、一致、可分页的快照读取能力，再完成混合负载统一验收。实现采用纯 Python，不以扩大超时、放松权限或降低测量负载替代优化。

## 1 实测依据与问题边界

### 1.1 原始失败基线

原始目录：`tradecore-python/联调测试结果/UDS-S5-MIX-FRESH-R1/UDS-S5-MIX-FRESH-R1/`。
其中三个时间目录均是 R1 尝试，不是 R1、R2、R3 三轮正式验收。最新目录为 `20261008T091821.475368Z`。

- 200,000 个合成事件预置成功。
- `summary.json` 为 `BLOCKED`，`failure_stage=preparation`、`measurement_started=false`。
- 4 个 SubmitSignal 预热请求成功，4 个快照和 2 个 PollEvents 请求超时。
- 未进入正式 30 秒窗口，不能从本轮给出 S5 实测 TPS/QPS 或通过结论。

附件中 trace 仅有 200,006 条；服务器同一 Journal 后续保留 200,012 条，新增的六条正是超时请求最终记录。服务器 trace 路径：

```text
/var/lib/tradecore-consumer-uds/traces/consumer-uds-f4b5d9b1da924adb998407d321c22289.jsonl
```

配套 `.status.json` 为 submitted=written=200012、queue_dropped=capacity_dropped=0、writer_failed=false。缺口源于归档时请求尚未收尾，不等同于 trace writer 丢失。

### 1.2 逐请求阻塞证据

下表为服务器同一时钟下的配对耗时，单位 ms。第一列时间从客户端发送完成至服务端 accept，不是客户端 connect 耗时。

| 请求 | 发送完成至 accept | frame ready 至 dispatch | handler |
| --- | ---: | ---: | ---: |
| 快照 1 | 0.104 | 0.025 | 6948.219 |
| 快照 2 | 7245.292 | 0.107 | 452.878 |
| 快照 3 | 7241.248 | 754.142 | 450.921 |
| 快照 4 | 7237.081 | 1509.176 | 439.680 |
| PollEvents 1 | 7217.376 | 2258.129 | 0.774 |
| PollEvents 2 | 7217.316 | 2259.198 | 0.457 |

六个请求最终均为 `closed_before_send_complete`。PollEvents 自身并非这六个请求的主要处理瓶颈；快照占用 reactor 后，其他请求先等待接纳，再等待 dispatch。

### 1.3 已确认机制与尚需归因部分

| 问题 | 代码位置 | 当前判断 |
| --- | --- | --- |
| 单次长 handler 不可抢占 | `consumer/service_v2.py` | 1 ms/64 请求预算在 handler 结束后检查，不能中断 6.95 秒请求 |
| 增量写入使全历史投影失效 | `consumer/uds_server_v2.py` | 尾部改变后 `_replay_items` 重建历史；`read_snapshot` 再汇总最新订单 |
| 热缓存仍返回全部订单 | `consumer/base.py`、`gateway_v2.py`、`dispatch_v2.py` | 每次仍遍历身份、构造全量响应，不能视为常数时间读取 |
| 全量响应超过单帧容量 | `consumer/wire_v2.py` | payload 上限 1,048,576 bytes；20 万订单无法装入单帧，检查却在全量 JSON 编码之后 |
| 编码异常没有明确业务回执 | `consumer/unix_v2.py`、`service_v2.py` | 部分错误直接关闭连接，trace 未保留足够的失败分类 |

`parallel_read_operations` 允许部分操作绕过 dispatch 锁，并不在当前 reactor 中自动创建并行执行者。首个快照 6.95 秒内的投影、对象构造、校验、GC 占比尚未分别采样；不能全部归为某一个函数。现有关闭记录也不等于已直接捕获 `FRAME_TOO_LARGE` 错误码。

预置阶段另有 SubmitSignal handler 约 1944.880 ms 的长尾，需独立归因。不能把快照修复当作已消除所有写入长尾。

## 2 目标与不可破坏的约束

1. SubmitSignal 保持单写者、Journal-first、幂等和明确接纳语义；不得因读侧变慢重复写入或重复调度。
2. reactor 不做全历史重建、全账户编码、同步等待子进程或磁盘刷盘。
3. 每个请求的 CPU 工作量、响应大小、排队量、保留状态都必须有界；不能只限制连接数。
4. 小账户现有全量快照语义保留；大账户不静默截断，不把部分数据包装成完整快照。
5. 快照固定版本与 watermark，权限仍逐请求校验；不能以缓存为由取消跨账户、epoch、generation 或过期校验。
6. 新快照分页和现有 PollEvents 的签名游标语义明确区分；现有 `CreateReplayPartitions` 不被改成快照分页接口。
7. 保持 `gateway_dispatched=false`，不调用 EMT/QMT。OMS-only 改进不升级为真实交易性能、恢复权威或 READY 证明。
8. 普通代码自检随开发进行；一个完整功能包完成后集中进行其功能与故障验收。全套正式压力验收在功能包完成后统一执行，避免每个小修改都重跑三轮。

## 3 目标架构

```text
同机 Consumer 客户端
  └─ 现有 UDS frontend
       ├─ framing / principal / binding / 有界分类调度
       ├─ SubmitSignal → 单写者 OMS → Journal 提交 → receipt
       └─ 读请求 → 有界异步 IPC → 独立 Python 读侧进程
                                      ├─ Journal 已提交前缀增量投影
                                      ├─ 有界版本化快照与分页索引
                                      └─ 固定 watermark 的事件回放
           响应经 frontend 有界发送队列返回客户端
```

外部 UDS 地址和 principal 授权方式保持不变。独立读侧进程隔离 Python/GIL 重计算，不新增第二个交易写者。前端不得阻塞等待 IPC；返回结果按 request/correlation 关联，原会话失效或客户端离开后不再交付过期结果。

内部 IPC 仅传经过授权的作用域、版本和有界页面，不传整份 20 万订单字典，不逐请求启动进程。内部端点需 ACL 和认证来源校验，不信任客户端自报 UID。读侧资源上限、崩溃重启和队列满错误必须独立可观测。

## 4 实施功能包与交付物

以下按原定功能包记录交付边界；当前实现与验收结论见第 13 节。

| 功能包 | 优先级 | 完整交付内容 | 集中验收出口 |
| --- | --- | --- | --- |
| S5-A 保护与诊断 | P0 | 快照提前门禁、明确错误、分段 trace、接纳及编码异常分类 | 大请求不再先构造全量再失败；错误可关联；安全边界不变 |
| S5-B 一致性分页合同 | P1 | schema、签名分页 token、快照版本、客户端辅助工具及文档 | 正反例、全量重建及快照转增量闭环通过 |
| S5-C 增量投影与执行隔离 | P1 | 增量索引、读侧进程、有界 IPC、版本发布和故障恢复 | 冷构建不阻塞写者；无越界、跳读、静默落后 |
| S5-D 公平调度与容量 | P2 | 操作配额、发送字节预算、慢读回收、容量 preflight | 饥饿、队列满、慢读、断连和资源上限测试通过 |
| S5-E 部署与统一验收 | 发布门禁 | Git 部署、安装包核验、完整功能回归、三轮 S5 与证据 | 逐项判定，不以单一 PASS 掩盖未通过指标 |

实施顺序：A → B → C → D → E。A 可以先部署作为保护，但明确拒绝不是 S3 成功；B 使用受控 fixture 完成合同测试后，由 C 提供真实增量实现，E 验证整体结果。

### 4.1 S5-A 保护与诊断

涉及 `dispatch_v2.py`、`wire_v2.py`、`unix_v2.py`、`service_v2.py`、`request_trace_v2.py`、`uds_server_v2.py`。

- 在已维护的订单数量、编码大小索引上进行 O(1) 预判；索引未就绪则显式拒绝或提示准备中，不为了判断容量先扫描全部订单。
- 固定订单数上限只能作为第一道限制；还必须使用有界编码器检查实际 UTF-8 字节预算。保留最终 wire 层 1 MiB 防线。
- 旧全量快照超限返回 `SNAPSHOT_REQUIRES_PAGINATION`；不能自动换成第一页并伪装旧响应。
- 预检查后仍发生编码超限时，返回小型、可关联的错误 frame；连接已不可写则保留准确终态，不能标成功。
- 将异常分类写入 trace：projection、authorization、payload build、encode、peer close、deadline、queue reject。异常文字使用脱敏白名单，不记录业务 payload/token。
- 对遗留同步冷构建先禁止在 reactor 执行；返回明确 `SNAPSHOT_NOT_READY`，随后由 C 实现受控后台准备。
- 为 SubmitSignal 的预置长尾补接纳、Journal commit、状态更新、发布通知等采样；GC/调度归因是旁路观测，不在交易线程加入同步 I/O。

集中验收：小快照仍返回完整旧合同；刚好低于/等于/超过字节边界、长 UTF-8 字段、20 万订单、缓存未就绪均有明确结果。超限请求不创建大体量中间响应、不消耗 Journal 写槽、不影响一个随后正常请求；不得用扩大客户端 5 秒 timeout 作为修复。

### 4.2 S5-B 一致性分页合同

操作 `CreateOrderSnapshot` 与 `ReadOrderSnapshotPage` 已进入 `contracts/consumer/v2/`、合同摘要和官方客户端。旧 golden vectors 保持；新增向后兼容 operation 按当前协商规则声明能力，旧服务不支持时明确报错。

| 操作 | 请求关键字段 | 响应关键字段 |
| --- | --- | --- |
| CreateOrderSnapshot | `consumer_account_id`、`page_size`，可选 freshness 条件 | `snapshot_id`、`journal_id`、`stream_generation`、`snapshot_position`、`order_count`、实际 `page_size`、初始 `page_cursor`、创建/过期时间、projection lag/stale 信息 |
| ReadOrderSnapshotPage | 不透明 `page_cursor` | 固定快照身份及 watermark、`orders`、实际 count、`has_more`、`next_page_cursor`；最后一页提供 `replay_cursor` |

初始压测候选 `page_size=100`，比较 100/200；实际单页受数量与响应字节双重限制。建议页面 payload 预算不超过 256 KiB，给 1 MiB 协议上限留余量；这是待验证配置，不是已达性能承诺。单条订单已超过页面预算则明确错误，不能产生不前进的空页循环。

分页 token 必须绑定 consumer/account、authorization epoch、snapshot_id、Journal/generation、固定 watermark、下一索引或排序键、页大小及有效期。分页使用稳定且唯一的订单排序键，不使用可变列表的裸 offset 跨版本读取。

一致性要求：

1. plan 固定视图；并发新增、改单、撤单不得改变已固定页面的内容或排序。
2. 每页必须来自同一已验证版本；旧版本被回收返回 `SNAPSHOT_EXPIRED` 或 `SNAPSHOT_GENERATION_MISMATCH`，禁止偷偷重建为新版本接续。
3. 最后一页的 `replay_cursor` 从该快照 watermark 之后开始消费，补齐期间新增事件；客户端持久化完整快照后才能推进消费 checkpoint。
4. 同一页 token 可重试并返回相同业务内容；不承诺 exactly-once 网络交付，调用方负责幂等应用。
5. 允许经明确 freshness 合同返回较早的已发布视图，但必须携带 lag/stale；不满足请求门槛则拒绝，不能假装最新。
6. 空快照可直接完成并提供对应增量起点；页数、唯一订单数及总数必须可核验。

安全校验可以在不可变版本发布时验证数据归属，并在每页复核作用域；每次请求仍检查当前 principal/binding/epoch。不能删除授权，只能避免重复扫描全账户数据。

集中验收：空、1 条、非整除分页、20 万/26 万订单、同 watermark 多事实、并发更新、页重试、串页、改 token、跨账户、授权撤销、过期、reset、最后一页与增量重放无缝衔接。最终重建结果与同一 watermark 的权威 Journal 投影逐项一致。

### 4.3 S5-C 增量投影与读侧隔离

保留 `uds_server_v2.py` 作为装配入口，建议新增 `snapshot_index_v2.py`、`snapshot_cursor_v2.py`、`read_worker_v2.py`，避免继续把全部能力堆进服务装配模块。

- 读侧按已验证的 Journal committed prefix 前进，维护 `last_applied_position`、按订单的最新状态和分页索引。
- 每次只处理 position 之后的事实；新增 4 笔不能触发对 20 万条历史重新投影。
- 版本发布时数据根与 watermark 原子对应；不允许先宣布新 watermark 再补数据。
- 使用有界分块/版本化索引复用未改变部分；不要为了每个查询复制全账户字典，也不要为每个新增订单全量排序。
- 同一构建任务 single-flight；创建快照只是引用已发布版本，不逐请求全历史构建。
- 页内容可以缓存编码片段，但 request_id/correlation、签名、权限和期限不能跨请求错误复用。
- 读侧独立进程只读 Journal，由明确的路径、权限、generation 和记录完整性校验约束；通知只是唤醒提示，缺通知时仍可从已提交前缀追赶，通知不能成为事实权威。
- 读侧崩溃、滞后、CRC/版本不合法时 fail-closed。重建期间写侧不得等待读侧；必要时读取拒绝，但不能提供错误成功。
- 版本保留、索引、IPC、编码缓存共同受字节上限约束。初始候选每作用域 2 个保留版本、8 个活跃快照、TTL 120 秒；超限显式拒绝，压测前冻结最终参数。

集中验收：投影等价、通知丢失、读侧 kill/restart、写侧持续提交、CRC 错误、generation 切换、版本回收、缓存失效、IPC 断开/超时、迟到结果、客户端取消。必须证明仅能恢复已验证的读视图，不将其宣称为全交易自动恢复或 READY。

### 4.4 S5-D 公平调度与容量

- 为 SubmitSignal、PollEvents、快照分别提供有界队列与在途配额；先验证请求类型与授权，再给予对应配额，未知操作不得伪装高优先级。
- 单写者顺序不变；读请求并行量有限。采用带保底服务份额的公平调度，而不是无限写优先导致读饥饿。
- 接纳、dispatch、编码结果、socket output 分别限制数量和字节；每轮发送有字节预算，慢读不能长期占有所有连接。
- 结果提交前确认会话仍有效，连接已断开时回收有界任务引用。取消读任务不得取消已经 durable accepted 的写入。
- 队列满返回明确背压，写请求发送后无回执仍按 unknown 处理，不能自动当作未提交重发。
- 窗口前计算 Journal 预算：seed、预热新信号、窗口新信号、冲突/其他事实和操作余量全部计入。

本轮 `fact_slot_budget=520008`、容量 524288，余量仅 4280。新门禁要求至少预留计划消耗的 10% 或 10000 fact（取较大者），不足时 BLOCKED；实际通过扩容还是调整明确的准备数据由部署 profile 冻结。扩容需同时核算 mmap、RSS/cgroup、磁盘和保留证据，不盲目翻倍或删除旧 Journal。

集中验收：慢读、半帧、客户端关闭、单类洪峰、读侧不可用、配额耗尽、文件描述符压力、容量临界、超时后迟到响应。过载测试可以拒绝，但不能把其混入额定 S5 的零失败通过结论。

## 5 观测与证据要求

新增 trace 字段按操作可选；不存在的阶段标缺失/不适用，不能填 0 冒充实测。

| 阶段 | 时间或计数 |
| --- | --- |
| 输入与调度 | 保留 accepted/frame_ready/dispatch；增加操作 lane、lane queue wait、inflight、pending bytes |
| 投影构建 | refresh begin/end、applied facts、source watermark、published watermark、cache hit/miss、lag |
| 快照读取 | version acquired、page read begin/end、page validation end、order count、page count |
| IPC | enqueue、worker begin/end、frontend completion observed，均为同主机 monotonic 时间 |
| 响应 | payload build begin/end、encode begin/end、encoded bytes、send finish 或明确关闭/错误阶段 |
| 客户端消费 | response validated、state apply、checkpoint commit、planned release、实际发送与完成 |
| 资源 | 分进程 CPU/RSS、minor/major fault、迁核、上下文切换、GC 旁路统计、队列/缓存高水位 |

报告每个区间 count/P50/P95/P99/P100，样本满足统一标准时增加 P99.9；整批墙钟独立统计。禁止通过相减两个总体分位数归因。

错误路径也必须保留 request SHA-256、失败阶段与最终状态。客户端超时不代表服务端 handler 已停止；归档应等待有界收尾并做一对一关联。超出收尾期限仍未结束的请求必须列为 unresolved，不能无限等，也不能把 status 中 queue_depth=0 当作所有请求均完成。

checkpoint 的完整 cursor 只存私有文件，公开证据使用摘要；原始 trace 不包含账户、策略业务键、订单内容、密码或 token。Journal 与 cursor 不能混用跨轮次身份。

## 6 正式压测口径

### 6.1 查询成本必须冻结

20 万订单全量快照每秒 5000 次，相当于每秒返回 10 亿条订单记录，不能与一页 100 条的 5000 QPS 合并统计。

| profile 类型 | 分子与单位 | 必须附带的成本 |
| --- | --- | --- |
| 原有小账户完整快照 | completed full-snapshot QPS | orders/snapshot、response bytes、账户数据量 |
| 新增大账户分页查询 | completed page QPS | 固定 page size、orders/s、bytes/s、版本命中与 freshness |
| 大账户完整重建 | complete snapshots、整批墙钟 | 全部页面、总订单数、唯一性、watermark、接续增量正确性 |

本方案建议混合性能使用有界页面查询，但**当前统一标准及旧 S5 结果不得由本文自动改成分页口径**。实现 B 时必须同步定义新 profile ID（建议 `UDS-S5-MIX-PAGED-FRESH`），更新接口规范与对应验收条目，并由双方冻结。未完成合同与 profile 对齐前，原 S5 继续标未通过；元数据查询、拒绝请求、空页和 full snapshot 均不可混计 page QPS。

### 6.2 正式混合目标

在双方冻结的新分页 profile 下，三个流使用同一 30 秒窗口，独立三轮：

| 流 | 负载与最低有效样本 | 延迟与完整性门禁 |
| --- | --- | --- |
| S1 | 2000 accepted TPS，至少 60000 唯一成功请求 | P95≤20 ms，P99≤60 ms；真实本地 receipt/Journal 对应，零错误副作用 |
| S2 | 5000 checkpointed unique event/s，至少 150000 | 完整消费至 checkpoint P95≤10 ms，P99≤40 ms；无 gap、未解释重复及 scope 内乱序 |
| S3 分页 | 5000 completed page QPS，至少 150000 有效页响应 | P95≤10 ms，P99≤40 ms；固定数据成本、正确版本和 freshness |

S3 固定版本页面的合法重复查询可计查询 QPS，但不能称为新增唯一订单吞吐。至少单独完成一次 20 万订单全量遍历和更新后的版本检查，不能只测空页、首条或永久陈旧缓存。版本创建请求单独计数和报告，不冒充页面读取。

S2 预置数据不把历史 seed 时间当作本轮消费释放时间；必须冻结合成 profile 的 `S0` 释放定义，记录计划释放到 checkpoint 的完整延迟，并单报 RPC、响应到处理、处理到持久化。正式 checkpoint 持久化不可省略；统一标准关于窗口内无额外磁盘 I/O 的文字须明确该必要提交的例外，不允许其他业务 I/O 混入。

额定混合门禁要求三流同时达标，零 dropped/failure、无无界队列、无未解释 lag；backpressure/NOT_READY/分页要求错误均不算成功。达不到时保留最高稳定档与首个失稳档，不降低目标后改写 PASS。

30 秒是计划释放窗口：S1/S2/S3 必须分别在该窗口内释放 60000/150000/150000 个目标单位，并最终全部成功。由于最后一个 10 ms release tick 与 P99 60/40/40 ms 延迟门禁客观重叠，`completed_in_window` 作为证据单列，但不要求最后一批在第 30.000000 秒前完成；收尾墙钟分别不得超过对应 P99 门禁，超过即失败。禁止以这一收尾规则接受少发、丢失、错误、无界积压或秒级 drain。

### 6.3 低并发与长时间回归

- 相同环境交错运行修改前后热态单笔、S1 单流、S2 单流、S3 单流，至少三轮，冻结 trace、CPU、数据规模和连接方式。
- 低并发 P95/P99 不得稳定劣化超过冻结基线的 10%；基线噪声无法判定则追加配对轮，禁止只挑最好一轮。
- 加入冷构建、增量写入、版本轮换和 10 分钟有界持续负载检查。持续负载按实际容量提前预算，不把短 30 秒的数据集无限循环用于唯一事件计数。
- 10 分钟结果不替代完整统一标准规定的其他 soak/fault 项，也不扩展为跨主机高可用验收。

## 7 集中验收矩阵

| 编号 | 测试 | 必须结论 |
| --- | --- | --- |
| V01 | 小快照旧客户端、旧 wire 向量 | 语义兼容，协议/hash 变化有明确声明 |
| V02 | 单条/整页/全量超限、编码错误 | 有界失败，可关联，不能静默截断或假成功 |
| V03 | 空、非整除、20 万/26 万分页 | 同版本总数与唯一性正确，完整快照可重建 |
| V04 | 分页同时持续 S1 写入 | 快照固定、增量可接续，不重复、不漏单 |
| V05 | 跨账号、篡改、epoch 撤销、过期 | 确定性拒绝，不泄露其他作用域内容 |
| V06 | reset、读进程重启、旧版本回收 | 旧 token 不越代次，恢复只发布验证后视图 |
| V07 | CRC/未提交尾部/通知丢失 | 不接受坏事实，通知丢失可追赶，不发布虚假 watermark |
| V08 | 大快照冷构建同时 S1/S2 | reactor 不出现秒级独占，S1/S2 按冻结目标验收 |
| V09 | 慢读、洪峰、IPC 满、断连 | 各层数量/字节有界，容量释放，无饥饿或错误成功 |
| V10 | 原预置 SubmitSignal 秒级长尾复现 | 完成归因；若仍破坏额定 SLA，不得签 S5 全通过 |
| V11 | 1 在途热态基线及单流三轮 | 不牺牲正常低并发，记录逐轮分位数和退化判断 |
| V12 | 新分页 S5 三轮同窗 | 三流吞吐、完整延迟、正确性、资源和证据全部通过 |
| V13 | trace 收尾及故障证据 | 正常/错误逐请求关联；有缺口就分项失败，不只检查数量 |

A 完成后集中 V01/V02/V13；B 完成后以 fixture 集中 V03/V04/V05/V06 合同测试；C 完成后在真实 Linux 上集中 V03–V08/V10；D 完成后集中 V09；E 统一执行全部矩阵、单流及混合压力。各包的静态检查和普通单测不能冒充 Linux 功能验收或正式性能结果。

## 8 代码与测试落点

| 目录或文件 | 实际修改 |
| --- | --- |
| `contracts/consumer/v2/` | 新 operation、错误码、schema/golden/capability 与合同摘要 |
| `consumer/dispatch_v2.py` | 保持旧接口，新增有界操作，不构造无界 orders |
| `consumer/base.py`、`gateway_v2.py`、`snapshot_v2.py` | 分页/版本类型、授权、一致性检查，避免每次扫描全账户 |
| `consumer/uds_server_v2.py` | 新读侧组合、冷/热状态及配置，不把重计算留在装配回调 |
| `consumer/service_v2.py`、`unix_v2.py` | 非阻塞 IPC completion、分类调度、限额和明确错误发送 |
| `consumer/wire_v2.py`、`request_trace_v2.py` | 有界编码、防线保留、阶段与异常证据 |
| `consumer/snapshot_index_v2.py`、`snapshot_cursor_v2.py`、`read_worker_v2.py` | 增量视图、签名分页、读进程、崩溃恢复及热窗 GC 隔离 |
| `consumer/client_v2.py`、`tools/performance/` | 官方分页/完整重建探针与证据汇总，不代替测试者 checkpoint |
| `tests/test_consumer*` | 正反例、故障矩阵、权限、容量、trace 与真实 Linux UDS |
| `deploy/consumer-uds/`、相关 systemd 配置 | 读侧生命周期、资源预算、配置和回滚说明 |

同步维护文档：ConsumerGateway V2 上游规范、真实 UDS 标准、服务器交接、统一性能标准、S2 分片说明。任何新合同/测试口径变更应在代码启用前同步，不让测试者依赖口头说明猜测。

## 9 发布与回滚

1. 功能包代码完整、自检和该包集中验收成功后，只提交相关变更并 push 当前对应分支；核验远端 commit。其他项目/QMT 变更不纳入本次修改。
2. 正式压测冻结代码、合同摘要、配置、安装包摘要和 generator revision。服务器仅 Git 拉取部署，更新专用 `.venv` 安装包，并验证 installed module 与 checkout 一致。
3. 部署前确认没有正在运行的正式窗口，记录旧 PID、启动时间和 Journal 身份；不重启整机、不修改 EMT/QMT 或其他项目服务。
4. 仅重启/更新独立 Consumer UDS 及其新增受管读侧进程。记录新 PID、Journal、generation、socket ACL、enabled/active 状态与资源限制。
5. 先做旧接口、小/大分页、旧 token 拒绝、读进程故障和恢复探针，再由测试方独立 reset、预置、签发本轮 plan。部署探针不与正式测量混用。
6. 回滚使用可验证的上一发布 Git revision 和安装包；不重置其他分支或删除工作区。停止新读侧并清理其临时运行句柄，保留 Journal、trace 和 checkpoint。
7. 回滚后旧大快照路径仍有已知风险，不能重新允许其无界请求或宣称旧 S5 已通过。新分页客户端遇到旧服务应明确不支持，不静默退回全量读取。

旧 Journal/签名 token 不因回滚自动恢复有效；是否可恢复数据必须遵循现有恢复合同，本方案不创造额外交易权限。

## 10 证据目录与状态判定

每个正式轮次建议保存：

```text
<run_id>/
  manifest.json                 # revision / schema / package / 配置摘要
  profile.json                  # 明确完整快照或分页，大小、版本和 freshness
  reset.json                    # 前后 PID / Journal / generation
  seed.json                     # 唯一事件数 / fact 容量预算 / 预热边界
  snapshot-plan.json            # 真实 plan 的脱敏版本，token 只留摘要
  snapshot-pages.ndjson         # 逐页边界、数量、字节和配对时间
  snapshot-integrity.json       # 完整重建、版本及增量衔接校验
  s1-samples.ndjson
  s2-samples.ndjson              # 含 checkpoint 提交时间和私有存储摘要
  s3-samples.ndjson
  server-trace.jsonl
  server-trace.status.json
  trace-coverage.json
  resources.jsonl
  per-second.json
  errors.ndjson
  summary.json
  sha256sums.txt
```

summary 分项列出 preparation、contract、integrity、throughput、RPC latency、checkpoint latency、full consume latency、resource bounds、trace coverage、dispatch isolation。只有全部适用门禁通过才给总 PASS；缺项为 INCOMPLETE，准备失败为 BLOCKED，正式窗口不达标为 FAIL，不能相互覆盖。

最终进度：

| 功能包 | 设计 | 代码 | 功能验收 | 正式压测 |
| --- | --- | --- | --- | --- |
| S5-A | 已定档 | 已实现 | 已通过 | 已纳入正式轮次 |
| S5-B | 已定档，profile=`UDS-S5-MIX-PAGED-FRESH` | 已实现 | 已通过 | 已通过 |
| S5-C | 已定档 | 已实现 | 已通过读进程 kill/recover 与 20 万完整性 | 已通过 |
| S5-D | 已定档 | 已实现，WRITE:SNAPSHOT 饱和权重 2:5 | 已通过 | 已通过 |
| S5-E | 已定档 | 已部署 | 已通过 | R1/R2/R3 通过 |

## 11 双方分工

TradeCore 负责服务端保护、分页合同、增量投影、调度隔离、权限与容量门禁、官方调用工具、部署及完整服务端 trace。

联调开发负责按正式合同采集数据、客户端状态/checkpoint 持久化、有界负载生成、预热与正式窗口分离、同窗三流、逐轮 reset、原始时间戳和完整归档。不能用固定 4096 worker 或无界任务队列代替实际负载建模；worker 数、进程数和在途量必须相互一致并可观测。

双方共同冻结大账户查询的页面成本与 profile 名称、合成消费的释放起点、freshness、容量及回归基线。只有调整报告字段且保留了原始响应时可离线修正；修改实现、负载或计时边界后必须重新测量，不改写历史失败结果。

## 12 关联规范

- [统一性能压测标准](../05-统一性能压测标准.md)
- [真实 UDS Consumer V2 压测与验收标准](./11-真实UDS-Consumer-V2压测与验收标准.md)
- [服务器真实 UDS 部署与压测方交接](./12-服务器真实UDS部署与压测方交接.md)
- [PollEvents 签名分片游标与 S2 联调说明](./14-PollEvents签名分片游标与UDS-S2-5K联调说明.md)
- [ConsumerGateway V2 上游策略联调接口规范](../../02-接口与数据/06-ConsumerGatewayV2上游策略联调接口规范.md)

## 13 实施、部署与验收实录

### 13.1 已落地能力

1. 旧全量大快照在扫描/编码前返回 `SNAPSHOT_REQUIRES_PAGINATION`；小快照旧合同保持。
2. `CreateOrderSnapshot`/`ReadOrderSnapshotPage` 使用固定版本、签名 cursor、作用域/epoch/generation/TTL 校验，最后一页提供增量 replay cursor。
3. 20 万订单固定视图已完成 2000 页重建，订单数及唯一性均为 200000；并发新增不污染旧版本，token 篡改确定性拒绝。
4. 冷构建在单独 `spawn` 读进程执行；kill 后父服务 PID 不变、写入继续接纳、读进程可重建，失败期间返回 `SNAPSHOT_NOT_READY` 而非错误成功。
5. 页面编码模板、签名 cursor 验证/续页结果均有界缓存；schema 快路径只在已审核摘要命中时启用，合同变化自动退回完整校验。
6. UDS reactor 按 WRITE/REPLAY/SNAPSHOT/OTHER 使用有界队列、输出字节预算与借用式公平调度。实测成本校准后，WRITE 与 SNAPSHOT 同时持续 ready 时为 2:5；空 lane 不保留闲置配额。
7. 1,000,000 fact 槽和 6 GiB cgroup 上限已部署。20 万 seed 加 6 万窗口写入使用 520000 槽，满足容量安全余量。
8. 原 3.356 秒写 handler 停顿已归因于 20 万长寿命对象参与全代循环 GC。专用 `NO_ADAPTER_BENCHMARK` 进程在服务启动、预置之前关闭自动循环 GC，引用计数始终生效，并在服务关闭时恢复原 GC 状态；请求收割路径不执行同步 `collect/freeze/unfreeze`。EMT/QMT 运行时不启用该模式。
9. `consumer_uds_mixed_probe.py` 使用三个独立 OS 进程生成 S1/S2/S3；S3 使用与单流基线相同的 asyncio 模型，避免客户端线程 GIL 被误判为 TradeCore 瓶颈。S2 每页 checkpoint 执行 `fdatasync` 后才记完成。

本轮关键提交依次为 `06eb5e6`（快照图 GC 隔离）、`24b0f64`（三进程混合 runner）、`0d8d3a1`（热窗自动循环 GC 隔离）、`3b26787`（S3 asyncio generator）、`07da49b`（最终服务份额）、`845dbdf`（有界收尾判定）和 `b6470a7`（systemd socket-ready 门禁）。

### 13.2 正式混合结果

固定条件：同机真实 UDS、20 万唯一订单、page size 100、30 秒计划释放窗口、S1 2000 accepted TPS、S2 5000 durable-checkpointed event/s、S3 5000 completed page QPS。每轮独立重启、独立 Journal、独立签名 token；错误数和 release drop 均为 0。

| 轮次 | 流 | 完成/唯一 | P50 ms | P95 ms | P99 ms | P100 ms | drain ms | 判定 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| R1 | S1 | 60000/60000 | 5.025 | 9.380 | 12.007 | 39.157 | 0.828 | PASS |
| R1 | S2 | 150000/150000 | 4.997 | 7.151 | 8.423 | 14.076 | 0.000 | PASS |
| R1 | S3 | 150000 | 5.809 | 9.737 | 12.501 | 39.412 | 0.963 | PASS |
| R2 | S1 | 60000/60000 | 5.209 | 10.360 | 12.317 | 33.101 | 0.000 | PASS |
| R2 | S2 | 150000/150000 | 4.860 | 6.329 | 7.118 | 16.698 | 0.000 | PASS |
| R2 | S3 | 150000 | 5.938 | 9.942 | 12.800 | 33.316 | 0.590 | PASS |
| R3 | S1 | 60000/60000 | 4.942 | 9.260 | 11.279 | 27.861 | 0.000 | PASS |
| R3 | S2 | 150000/150000 | 4.881 | 6.092 | 6.991 | 19.454 | 0.000 | PASS |
| R3 | S3 | 150000 | 5.772 | 9.652 | 11.864 | 21.579 | 0.000 | PASS |

R1 的原始 load revision 为 `07da49b`。`845dbdf` 只把最后 release tick 的判定改为第 6.2 节有界 drain，未改变请求、释放、计时或服务端代码；离线复核文件明确记录 `traffic_generator_unchanged=true`，所有检查为 true。R2/R3 直接由 `845dbdf` 生成 PASS。

冷快照发布耗时为 R1 `9.216 s`、R2 `8.823 s`、R3 `8.997 s`，全部位于测量窗口之前。三轮 cgroup `MemoryPeak` 分别为 4657586176、4658163712、4656545792 bytes，均低于 6 GiB；各轮 `NRestarts=0`。

### 13.3 trace、回归与证据

- R1 trace status：submitted=written=411719、queue/capacity dropped=0、writer_failed=false、peak_queue=24。
- R2 trace status：submitted=written=411721、queue/capacity dropped=0、writer_failed=false、peak_queue=51。
- R3 trace status：submitted=written=411717、queue/capacity dropped=0、writer_failed=false、peak_queue=39。
- 本地完整回归：`1043 passed, 34 skipped, 308 subtests passed`；新增/受影响文件 Ruff 与 Pyright 均为零问题。
- R2/R3 预置各有一次连接前 `FileNotFoundError`，分别 730/521 条；均无 `sent_ns`、无 unknown commit，使用新 request ID 精确补齐后再创建 20 万固定快照。根因是原 systemd `Type=simple` 的 restart 返回早于 pathname socket bind，并非服务崩溃；`b6470a7` 增加 `ExecStartPost`，只有 socket 类型、owner/group 和 `0600` mode 就绪后 restart 才成功返回。最终部署后紧接 restart 的只读 `ListAccounts` 已返回 `COMPLETED`。

服务器私有证据：

```text
/var/lib/tradecore-consumer-uds/s5-formal-R1-07da49b/mixed-R1/
/var/lib/tradecore-consumer-uds/s5-formal-R2-845dbdf/mixed-R2/
/var/lib/tradecore-consumer-uds/s5-formal-R3-845dbdf/mixed-R3/
/var/lib/tradecore-consumer-uds/traces/consumer-uds-912d900bc82f4f9f956d9fdd1af3143f.*
/var/lib/tradecore-consumer-uds/traces/consumer-uds-6cf4f958a7054c95a70c73ffe3c3b676.*
/var/lib/tradecore-consumer-uds/traces/consumer-uds-f83bc0b043074274bb016a1c6647cfff.*
```

当前结论只证明 server-local、无 Adapter、OMS 合成事实下的分页快照隔离和混合容量通过。它不证明真实 EMT/QMT 柜台吞吐、跨主机网络、整机掉电恢复或交易 `READY`。

### 13.4 2026-10-09 准备阶段与 reactor 增量回归

联调侧新证据在 20 万条预置后反复阻塞于 preparation：旧实现第一次 `CreateReplayPartitions` 延迟约 `4244.801 ms`，随后 snapshot 轮询最终触发 5 秒客户端超时。根因是 replay 计划第一次请求才扫描完整 Journal，以及快照 future 完成后的请求线程同步执行全代 GC；它不是 UDS 背压、OOM 或服务重启。

`13f4e8c` 将已提交事件增量发布到只读 replay/route 投影视图，并把专用 benchmark 的自动 GC 隔离提前到服务启动；`9063722` 将 reactor ready 集合限制为 32 条高水位，并把每轮分发预算由 1 ms 调整为 3 ms，避免 10 ms 微突发先堆满 128 条再处理。两项修改均保持有界队列、WRITE:SNAPSHOT=2:5、公平性、Journal-before-receipt、分页合同和 fail-closed 语义。

当前服务代码三轮独立 reset 的准备门禁结果如下。每轮均先以 2500 TPS 预置 200000 条唯一订单，再在 5 秒单请求上限下创建 32 个 replay partition 并取得 200000 条固定快照：

| 轮次 | replay event | snapshot order | snapshot 完成 ms | 最大单次调用 ms | 判定 |
| --- | ---: | ---: | ---: | ---: | --- |
| R1 | 200000 | 200000 | 3826.548 | 796.420 | PASS |
| R2 | 200000 | 200000 | 3851.444 | 768.021 | PASS |
| R3 | 200000 | 200000 | 3740.808 | 730.000 | PASS |

同一组三轮的服务端 trace 表明 reactor 优化有效：ready 队列峰值从 128 降为 31；R1 的 SubmitSignal/PollEvents/ReadOrderSnapshotPage 从 frame ready 到 send complete 的 P95 分别为 `6.313/2.121/4.246 ms`，R3 分别为 `5.939/1.774/4.075 ms`，handler P95 分别不超过 `0.238/0.816/0.039 ms`。三流完成数、唯一性和错误门禁均通过，服务无 restart、无 dropped trace。

同机 Python 端到端累计延迟尚不能把本次增量回归升级为新的三轮全量 S5 PASS：S3 P95 三轮为 `14.803/11.811/12.232 ms`，超过 `10 ms`；S2 在 R1/R3 也受 durable checkpoint 与同机调度影响超过 `10 ms`。服务端已经发送后的生成器调度/解析时间必须作为独立测试端问题保留，不能通过降低门限、改计时起点或挑选最好轮次消除。单独尝试多进程 S3 生成器后尾延迟反而恶化，相关代码已回退，只保留测量窗口 GC 隔离。

因此当前定档为：准备阶段故障 **已关闭**，TradeCore 服务端 reactor 指标 **已通过增量验收**，历史三轮正式 S5 PASS 证据继续保留；当前 revision 的客户端累计三轮门禁 **未重新签发**。联调测试端可基于已部署版本重新执行独立三轮，新的三轮全部达标前不得用本节替换第 13.2 节证书。

增量证据位于：

```text
/var/lib/tradecore-consumer-uds/s5-prep-fix-13f4e8c/
/var/lib/tradecore-consumer-uds/s5-reactor-9063722/
/var/lib/tradecore-consumer-uds/s5-formal-ea4ceb1/
```

增量代码完整回归为 `1066 passed, 34 skipped, 308 subtests passed`；Linux 安装产物的 read-worker、真实 UDS 与 reactor 回归为 `13/13`，Ruff 与 Pyright 均为零问题。

### 13.5 2026-10-09 独立 FRESH R1 失稳归因与发压门禁

独立联调证据 `UDS-S5-MIX-PAGED-FRESH-R1/20261009T111712.608954Z` 正确判为
`FAIL / INCOMPLETE_U0_U6_TRACE`，但不能据此把 TradeCore 服务端容量写成
S1 `1596.167 TPS`、S2 `246.667 event/s`、S3 `1763.233 QPS`。该轮实际使用
S1 2047、S2 2、S3 2047 个 worker，并声明 `queue_unbounded=true`。S1 和 S3
分别有 9644、94658 个已计划请求未在 30 秒窗口内开始调用；S3
`offered -> call start` P50/P95 已达到 `22550.078/23550.095 ms`，属于压测器
债务，不是 TradeCore handler 时间。

完整服务端 trace 覆盖的请求中，S1/S2/S3 handler P95 分别为
`0.230/0.690/0.033 ms`，frame-ready 到 dispatch P95 分别为
`17.253/2.759/2.694 ms`；服务 PID、启动时间与 revision 保持不变，trace writer
零 drop。1798 个 `POST_SEND_NO_RESPONSE` 均在 connect/send 成功后收到
`ECONNRESET`，且与 S1/S3 trace 缺口精确相等。旧证据没有同窗 ingress counter，
因此只能判定为“与 pre-admission connection saturation 高度一致”，不能伪造
更细的服务端根因。

当前正式工具和服务端证据合同固定为：

1. 混合 S1/S3 和单流 S3 负载队列最多容纳两个 worker wave，不再以
   32768/4096 条队列掩盖秒级积压；S2 每个签名分片最多积压两个 PollEvents
   请求。fresh S1 单流的默认在途上限由 4096 收紧为 256，硬上限为 1024；
   达到上限必须记录 `GENERATOR_OVERLOADED`，不能进入隐藏等待队列。
2. 每流必须输出 queue capacity、peak depth、call-start 样本数和
   `offered -> call start` P50/P95/P99/P100；任一 release drop、call-start 缺口或
   queue delay P99 超过一个 10 ms release tick，generator 门禁失败。
3. 开启正式 trace 时，`.status.json` 异步附带 socket accept、admitted、overload、
   overload reply/close/failure、lane reject、preframe/ready/output 峰值和所有固定容量。
   下一轮可用窗口前后 counter delta 区分压测器排队、pre-admission 饱和和服务端
   lane backpressure，不再依赖 connect 成功或缺失 trace 反推。
4. 该轮保留为 FRESH 首失稳档，不替换第 13.2 节历史证书。应用新门禁后必须
   独立 reset 并重跑 R1；R1 三流全部通过后才能继续 R2/R3。
5. 混合工具不再用固定 `FRESH` 名称掩盖连接复用。运行时必须显式选择
   `fresh-per-request` 或 `persistent-per-vu`，证据分别写入
   `UDS-S5-MIX-PAGED-FRESH` 或 `UDS-S5-MIX-PAGED-PERSISTENT`，两种结果不得
   拼轮、合并分位数或互相替代。第 13.2 节既有内部三轮实际使用连接复用，保留为
   `persistent-per-vu` 历史基线；不能将其作为 FRESH profile 的通过证书。

### 13.6 2026-10-12 FRESH accept 饥饿修复与部署验收

联调证据 `联调测试结果/latest-failed/latest-failed` 判定为生成器失效与服务端
accept 饥饿同时存在，不能把全部失败归给任一单方：

- 客户端采用 `fresh-per-request`，S1/S3 分别有 23914/72677 条请求在客户端
  有界队列返回 `QUEUE_FULL`，S2 另有 389 条；这些请求没有进入 TradeCore，
  不得计为服务端容量失败。三个流的 `offered -> call start` P99 分别约为
  356/325/333 ms，压测器门禁失败。
- 服务端同期仍出现 931 个 `overloaded_connections`，且全部属于
  `overload_closed_without_response`；这部分是真实服务端失败，不能用生成器失效掩盖。
- 对 112480 条客户端/服务端同源时间戳进行逐请求配对后，connect 完成至 server
  accept 的 P50/P95/P99/P100 约为 `90.887/150.332/177.726/264.926 ms`；
  server accept 至 send 的 P50/P95/P99 约为 `2.404/13.716/19.876 ms`，
  send 至客户端完成的 P50/P95/P99 约为 `0.201/0.822/1.408 ms`。主尾延迟位于
  内核 accept 队列等待，不在 OMS handler。

根因是旧 reactor 每轮先批量 accept 新连接，并在 ready 达到 32 后提前结束事件处理。
fresh 客户端收到响应后关闭形成的 EOF 与待写响应没有优先级，已完成连接长期残留在
selector 的 `states` 中；持续突发最终占满 6144 总连接门禁，并把健康连接留在内核
accept 队列。修复提交 `5934391` 采用以下有界调度顺序：

1. 每轮先处理已建立连接的待写响应和 EOF，及时回收 fresh peer；
2. 再读取已 accept 的首帧，最后才按 128 条预算 accept 新连接；
3. ready 高水位、单轮分发条数和 CPU 时间预算分别固定为 96、96、6 ms；
4. 保留 2048 pending、4096 preframe、1024 overload reply、64 MiB 输出预算、
   lane 公平队列、连接内顺序、Journal-before-receipt 和 fail-closed 语义。

Linux 专项回归一次持续发送 7000 个 fresh SubmitSignal，超过旧总连接容量，结果为
`7000/7000 ACCEPTED`、`overloaded=0`、`lane_rejected=0`、最终
`active_connections=0`，墙钟 3.142 秒。服务器受影响模块为 `7/7` 通过；本地受影响
集合为 `25 passed, 9 skipped`，Ruff、Pyright 和 diff 检查均通过。

服务器仅通过 Git 拉取并安装 CPython 3.10 wheel，部署 revision 为
`59343915ba4efe683029b32218c5e583aa51144b`。受控重启前后 PID 为
`2733089 -> 4146924`，服务保持 `active/running`，`NRestarts=0`。正式服务的独立
fresh 诊断在 1 秒预热后以 2000 TPS 运行 5 秒：测量窗口
`10000/10000 ACCEPTED`、窗口内 `1999.6 TPS`、唯一 LSN 10000，端到端延迟
P50/P95/P99/P100 为 `0.494/1.030/1.739/8.927 ms`。

随后混合探针和诊断请求合计使服务接纳并完成 54726 个连接；最终 counter 为
`accepted=served=54726`、`overloaded=0`、`lane_rejected=0`、
`active=preframe=pending=0`，trace `submitted=written=54726`、零 drop、writer
正常。仓库内置的短时混合探针仍因自身 128 深度队列产生 S1/S3 release errors，
故该轮只用于服务端无拒绝与资源回收验收，不能签发正式三流吞吐 PASS。

当前结论：TradeCore 本轮 accept 饥饿缺陷已修复、提交、部署并通过专项验收；
`latest-failed` 历史失败证据保留。正式 `UDS-S5-MIX-PAGED-FRESH` 仍须由联调方使用
已冻结的独立分片 generator，逐轮受控 reset 后重跑 R1/R2/R3；任一轮 generator
门禁失败、服务端 overload 非零或 trace 不完整，均不得记为 PASS。

### 13.7 2026-10-12 FRESH 微批排队收敛与部署验收

联调方在 `43efae6` 上重新执行的 `latest-failed` R1 证明第 13.6 节连接容量修复生效：
窗口及收尾阶段新增 `181300` 个连接全部被接纳和完成，服务端
`overloaded=lane_rejected=overload_closed_without_response=0`，trace
`submitted=written=393068`、零 drop、writer 正常，PID 及启动时间稳定且
`NRestarts=0`。因此该轮不能再归因为 UDS listener 容量不足。

该轮仍不能判为正式性能 PASS。S1/S2/S3 分别有 `8788/449/22173` 个请求在
客户端有界队列返回 `QUEUE_FULL`，`offered -> call start` P99 分别为
`171.130/124.959/136.257 ms`；这些请求没有到达 TradeCore。已到达服务端的请求
全部具有唯一 trace，但 S1/S3 的客户端调用 P95 仍为 `83.777/67.189 ms`。

服务端分段进一步定位到业务 ready 微批：S1/S3 的 handler P95 只有
`0.235/0.027 ms`，而 `frame -> dispatch` P50/P95/P99 分别为
`23.570/27.983/31.221 ms` 和 `8.052/11.600/13.257 ms`。该轮 ready 深度长期
贴近 96：两类请求的 pending P95/P99 均为 `94/95`。第 13.6 节为消除连接泄漏而
同时把 ready 高水位扩大到 96，虽解决了 overload，却形成可观测的批次锯齿排队。

提交 `82a52f7` 保留“先发送响应并回收 EOF、最后 accept”的连接生命周期修复，
只把业务 ready 高水位、单轮 dispatch 数和 CPU 预算从 `96/96/6 ms` 收紧为
`32/32/3 ms`；accept 预算仍为 128，其他固定容量、lane 公平性、连接内顺序、
Journal-before-receipt 及 fail-closed 语义均未改变。专项测试额外要求峰值业务 ready
不超过 32，防止以后重新放大微批。

本地受影响集合为 `7 passed, 9 skipped`，Ruff、Pyright 和 diff 检查通过。
Linux 一次持续 7000 个 fresh 请求的专项回归为 `7000/7000 ACCEPTED`，墙钟
2.965 秒，超过 6144 总连接容量后仍零 overload、零 lane reject、最终连接归零；
服务器受影响模块 `7/7` 通过。

服务器仅通过 Git 拉取并安装 CPython 3.10 wheel，部署 revision 为
`82a52f779c3375a9ed6b5e6f53cdba8f97323300`，受控重启 PID 为
`21945 -> 180075`，服务保持 enabled、`active/running`、`NRestarts=0`。独立
fresh 诊断在 1 秒预热后以 2000 TPS 运行 5 秒，测量窗口
`10000/10000 ACCEPTED`、窗口内 `1999.8 TPS`、唯一 LSN 10000，客户端调用
P50/P95/P99/P100 为 `0.473/0.967/1.659/20.798 ms`。

部署后的短时三流混合探针中，服务端分段改善如下：

| 流 | 样本 | frame→dispatch P50/P95/P99 ms | accept→send P50/P95/P99 ms |
| --- | ---: | ---: | ---: |
| S1 SubmitSignal | 9980 | 5.192/9.638/11.206 | 5.409/9.869/11.424 |
| S2 PollEvents | 250 | 0.665/1.307/1.493 | 1.163/1.857/2.252 |
| S3 ReadOrderSnapshotPage | 20393 | 1.768/4.308/5.444 | 1.837/4.420/5.708 |

相对联调失败证据，S1/S3 `frame -> dispatch` P95 分别下降约 65.6% 和 62.9%。
该探针期间服务端累计 `accepted=served=42795`、`overloaded=0`、
`lane_rejected=0`、trace 零 drop，ready 峰值精确受限于 32。仓库内短时生成器的
S1 已达到 10000/10000、零 release error 且排队 P99 为 1.054 ms；S3 仍受该工具
单进程队列限制，不能据此签发三流正式 PASS。

当前结论：TradeCore 服务端连接容量与业务微批排队两项缺陷均已关闭并完成部署验收。
下一步只需要联调方基于 `82a52f7` 独立 reset 后重跑正式 R1；只有 generator
`call_started=offered`、零 release error、排队 P99≤10 ms，且三流吞吐、延迟、
checkpoint、trace 和服务端 counter 全部达标，才能继续 R2/R3。

### 13.8 2026-10-13 FRESH 单请求连接收尾优化与部署验收

联调方在服务端代码 `82a52f7` 上提交的新一轮 `latest-failed` 进一步证明业务微批已
不再是主瓶颈。30 秒窗口内服务端接纳并完成 `176782` 个请求，`overloaded=0`、
`lane_rejected=0`、`active=pending=preframe=0`，trace `submitted=written=388545`
且零 drop。S1/S2/S3 的 handler P95 仅为 `0.240/0.640/0.030 ms`，S1/S3
`frame -> dispatch` P95 已收敛到 `9.78/4.03 ms`。

该轮仍为 FAIL：客户端 S1/S2/S3 分别有 `4101/590/31385` 个 `QUEUE_FULL`，窗口
完成吞吐只有 `1850.97 TPS / 3013.33 event/s / 3928.93 QPS`。逐请求同源时间戳表明，
已启动调用的主要累计延迟位于客户端 connect 完成至服务端 accept：S1 的
P50/P95/P99 为 `62.833/68.984/72.267 ms`，S2 为
`61.894/67.995/70.974 ms`，S3 为 `62.857/69.023/72.480 ms`；服务端 handler
不具备解释该数量级长尾的能力。

正式 profile 是一连接一请求，但旧服务在完整响应发出后仍把连接重新注册为 READ，
再等待客户端 EOF 并进入下一次 selector 回合。提交
`83f0fc8d56ea1430eae90b45aaceab45ade973b2` 增加受控配置
`max_requests_per_connection`：

1. 正式 `fresh-per-request` 配置固定为 `1`；完整响应发送完毕后由 reactor 立即关闭
   并回收连接，不再为观察 peer EOF 消耗一次 selector 生命周期；
2. 显式 persistent profile 可采用经评审的 `2..1024`，缺省仍为 `1024`，因此旧部署
   不会被静默改写；
3. wire schema、UID/GID 鉴权、lane 公平队列、Journal-before-receipt、响应完整发送、
   有界容量和 fail-closed 语义不变；
4. 新增真实 AF_UNIX 回归，验证请求得到 `COMPLETED` 后立即读到 EOF，且
   `accepted=served=1`、最终 active 为 0。

本地 Ruff、Pyright、diff 检查均通过；受影响单测为 `7 passed, 10 skipped`，其中
Windows 跳过 Linux AF_UNIX 权限用例。Linux 服务器完整 UDS 专项为 `8/8` 通过，
其中继续包含 7000 个持续 fresh 请求、零 overload、最终连接归零回归。

服务器只通过 Git fast-forward 拉取 `dev` 并安装 CPython 3.10 wheel；受控配置以
`root:xuanling 0640` 安装，未改账户、Adapter 或交易进程。服务 PID 从 `3011111`
变为 `3364369`，启动时间为 `2026-10-13 09:57:35 CST`，保持 `active`、
`NRestarts=0`。

部署后独立 2000 TPS fresh 诊断在 1 秒预热后运行 5 秒，测量窗口结果为
`10000/10000 ACCEPTED`、唯一 LSN `10000`、窗口完成 `1999.8 TPS`、零拒绝和零超时；
端到端 P50/P95/P99/P100 为 `0.482/1.043/1.703/7.987 ms`。这说明单流下旧轮
60～70 ms 的连接生命周期长尾已被消除。

随后短时混合诊断共向服务端实际启动 `27127` 个调用，服务端全部接纳并完成，最终
active/pending/preframe 均为 0，零 overload、零 lane reject、trace 零 drop。共同窗口
的服务端分段如下：

| 流 | trace 样本 | frame→dispatch P50/P95/P99 ms | handler P95 ms | accept→send P50/P95/P99 ms |
| --- | ---: | ---: | ---: | ---: |
| S1 SubmitSignal | 9960 | 6.851/12.305/14.035 | 0.264 | 7.083/12.563/14.316 |
| S2 PollEvents | 249 | 0.596/1.375/2.162 | 0.667 | 1.354/2.272/2.972 |
| S3 ReadOrderSnapshotPage | 16715 | 1.638/4.252/5.891 | 0.059 | 1.713/4.471/6.263 |

仓库内短时混合发生器只有每流一个 Python 进程，S3 自身仍发生 8123 个 release error，
且冻结数据只有 24000 条，不能用来签发正式三流吞吐证书。当前定档为：TradeCore 的
单请求连接收尾优化已提交、部署并通过专项验收；联调方必须在独立 reset 后使用原有
多进程 generator 重跑正式 R1。只有 call-start 覆盖、三流吞吐/延迟、checkpoint、
逐请求 trace 和窗口 counter 同时通过，才允许继续 R2/R3，历史 `latest-failed` 不得
改写为 PASS。

### 13.9 2026-10-13 FRESH complete-frame selector 优化与部署验收

联调方在最终 revision `af08257` 上提交的 `latest-failed(1)` 仍为有效 FAIL，且没有
复现服务端容量拒绝。服务在窗口及审计范围内增量接纳并完成 `173528` 个请求，
`overloaded=lane_rejected=overload_closed_without_response=0`，结束时
active/pending/preframe 全部归零，trace writer 零 drop、服务 PID 和 revision 稳定。

失败仍首先包含生成器门禁失败：S1/S2/S3 分别只有 `54132/890/116362` 个调用实际
开始，对应 `5868/610/33638` 个本地 `QUEUE_FULL`；`offered -> call start` P99 分别为
`164.177/149.455/148.999 ms`。因此缺失的 `40116` 个请求没有进入 TradeCore，不能
记作服务端处理失败。已开始的调用中，S1/S2/S3 客户端 P95 分别为
`78.287/71.144/71.996 ms`，仍未达到正式延迟门禁。

服务端分段没有出现业务 handler 数量级退化：S1/S2/S3 handler P95 分别为
`0.221/0.630/0.032 ms`，frame→dispatch P95 分别为 `10.575/2.064/3.739 ms`。
资源证据显示服务进程在30秒测量窗口使用约 `90.79 CPU seconds`。同期
`ready_on_accept=384110/384526`；对于测量增量同样超过 99% 的 fresh 请求，完整首帧
在 `accept()` 返回时已经可读。

旧 reactor 即使在 accept 后同步读出了完整帧，仍先将 socket 注册到 selector；入有界
ready lane 后再注销。正式一连接一请求 profile 因此每笔额外执行一对没有等待价值的
epoll add/delete 系统调用。提交 `fbdf6432b23b04bd56a367f062b8d3a07648cf9b`
改为：

1. 新连接只有在首帧不完整时才注册 READ；完整帧直接进入原有有界公平 lane；
2. 连接一旦形成完整待处理帧即解除 selector 关注，避免 queued socket 重复产生
   readiness 事件；
3. 只有部分写才注册 WRITE；显式 persistent profile 在响应完成后重新注册 READ；
4. close 对已注册和未注册连接统一处理，连接容量、响应完整性、慢读/部分写、
   Journal-before-receipt、UID/GID 鉴权和 fail-closed 语义不变。

本地 Ruff、Pyright、diff 检查通过；本地受影响集合为 `7 passed, 10 skipped`，Linux
实际 AF_UNIX、7000 fresh、慢读/部分写、persistent 流水顺序和 lane 公平性共
`17/17` 通过。服务器通过 Git fast-forward 拉取并安装 CPython 3.10 wheel，受控重启
PID 为 `769223 -> 1001855`，启动时间 `2026-10-13 13:37:09 CST`，保持 enabled、
active、`NRestarts=0`。安装包与 checkout 的 `service_v2.py` SHA-256 均为
`d5fe8963ba172aff0ebebf082e0137d0cd7525538fe23fe3402afcb4bf7858d9`。

部署后低负载基线为 1 秒预热、5 秒测量、2000 offered/s：`10000/10000 ACCEPTED`、
窗口完成 `1999.8 TPS`，P50/P95/P99/P100 为
`0.468/0.951/1.493/15.192 ms`，证明本次优化没有牺牲低并发主路径。

使用同一单进程诊断工具做修改前后饱和侧 A/B；该工具本身在高压下会
`GENERATOR_OVERLOADED`，因此只比较完全相同发生器下的相对变化，不签发正式吞吐证书：

| offered | 指标 | 修改前 | 修改后 | 变化 |
| ---: | --- | ---: | ---: | ---: |
| 5000/s | 最终 ACCEPTED | 19291 | 20605 | +6.8% |
| 5000/s | 窗口完成 TPS | 3678.2 | 3942.2 | +7.2% |
| 5000/s | P95 | 244.409 ms | 227.786 ms | -6.8% |
| 7000/s | 最终 ACCEPTED | 19252 | 19976 | +3.8% |
| 7000/s | 窗口完成 TPS | 3670.6 | 3855.2 | +5.0% |
| 7000/s | P99 | 272.623 ms | 254.573 ms | -6.6% |

三轮诊断后服务累计 `accepted=served=62575`，零 overload、零 lane reject、最终连接
全部归零，trace `submitted=written=62575`、零 drop、writer 正常。以上证明本次修改
有效降低系统调用和排队开销，但内置单进程高压发生器不具备正式三流签发资格。
联调方仍须在独立 reset 后，以既有13进程发生器重跑 R1；本节不预先宣称 S5 PASS。

### 13.10 2026-10-13 Journal/trace CPU 收敛与部署验收

联调方在 `60fe785` 上提交的证据 `20261013T054853.002214Z` 仍为有效 FAIL，但没有
TradeCore 接入拒绝。30 秒窗口及收尾阶段服务新增接纳并完成 `176742` 个请求，
`overloaded=lane_rejected=0`，最终 active/pending/preframe 均为 0；服务端 trace 完整、
零 drop。S1/S2/S3 分别只有 `55204/901/118254` 个调用实际开始，客户端本地
`QUEUE_FULL` 为 `4796/599/31746`，窗口吞吐为
`1827.47 TPS / 2986.67 event/s / 3916.77 QPS`。缺失请求没有到达 TradeCore，不能
归类为 UDS listener 拒绝。

逐请求同源时间戳显示，S1/S2/S3 的 connect 完成至 server accept P50 约为
`62.488/61.903/62.509 ms`，而 server accept 至 send P50 约为
`6.857/1.414/1.809 ms`。服务进程在约 30.081 秒窗口使用约 31.62 CPU 秒，已经接近
单核饱和。因此本轮不再扩大 pending/preframe/客户端队列，而是针对函数级 CPU 证据
收敛以下开销：

1. mmap Journal 使用依赖中已锁定的 `msgspec` 确定性 JSON 编码；每个 4096-byte
   frame 只写实际 header/payload/CRC，未使用的预分配区域保持零，不再为每个事实从
   Python 复制完整 4088-byte body；固定末尾 commit word 仍最后写入，torn tail 仍
   fail-closed。
2. Journal CRC 改为 header/payload 增量计算，避免一次额外拼接；恢复扫描增加显式
   字段类型检查，并验证 UTF-8 事实、稀疏 padding、重开续写和破损 commit 拒绝。
3. request trace 保持逐请求完整字段和 JSONL 合同不变，改用原生确定性编码，并将每批
   最多 256 条合并为一次文件写；容量/drop/status 门禁不变。
4. 高频文本验证先用 C 层 `isprintable()` 跳过绝大多数无控制字符的正则扫描；含 tab
   或其他非打印字符的输入仍落回原有精确规则。固定 principal 和 V2 version range
   改为启动期构造，UID/GID 检查仍逐连接执行。

同一 Linux 主机、同一 4000 条直接 S1 profile 的函数剖析中，修改前后总墙钟从
`1.542 s` 降至 `1.445 s`（约 -6.3%）；Journal `append_batch` 累计时间从
`0.162 s` 降至 `0.100 s`（约 -38.3%），事实编码从 `0.097 s` 降至 `0.037 s`
（约 -61.9%），`require_text` 从 `0.298 s` 降至 `0.263 s`（约 -11.7%）。这是隔离
函数基准，不替代正式三流吞吐结果。

代码提交 `b4468bd4504044a2143e50829e72ef0d4225f40b` 已推送 `dev`。本地完整测试为
`1068 passed, 37 skipped, 308 subtests passed`；受影响模块 Ruff、Pyright 为零错误。
Linux 使用运行环境完成 38 项真实 AF_UNIX/Journal/trace/OMS 回归，全部通过。服务器
仅通过 Git fast-forward 和本机构建 wheel 部署，服务 PID 从 `1101686` 变为
`3184893`，保持 enabled、active/running、`NRestarts=0`。

部署后低负载基线为 1 秒预热、5 秒测量、2000 offered/s：窗口
`10000/10000 ACCEPTED`、`1999.8 TPS`、唯一 LSN 10000，P50/P95/P99/P100 为
`0.430/0.785/1.437/8.609 ms`。服务同期 `accepted=served=12001`、零 overload、
零 lane reject、最终连接归零；trace `submitted=written=12001`、零 drop、队列归零。

当前结论：代码与部署专项验收通过，低负载没有退化；正式 S5 仍必须由联调方独立
reset 后重跑 R1/R2/R3。不得用上述隔离剖析或单流基线替代
`2,000 TPS + 5,000 checkpointed event/s + 5,000 QPS` 的完整联合门禁。

### 13.11 2026-10-13 单请求鉴权与 binding 索引收敛

联调方在 `1762aa0` 上提交的 `20261013T085503.902417Z` 为有效 FAIL，但失败边界仍在
生成器而非 TradeCore 接入容量。S1 最终 `60000/60000` 成功，窗口完成
`1996.43 TPS`；S2 只有 `1108/1500` 个调用启动，客户端 `QUEUE_FULL=392`，窗口完成
`3680 event/s`；S3 只有 `141615/150000` 个调用启动，客户端 `QUEUE_FULL=8385`，
窗口完成 `4699.57 QPS`。因此该轮 generator gate 为 `GENERATOR_INVALID`，不得把未
启动的 `8777` 个请求归类为服务端失败。

服务端在测量、drain 与 coverage audit 范围内增量接纳 `205166` 个连接，
`overloaded=lane_rejected=0`；完整运行累计 `submitted=written=415646`，trace 零 drop、
writer 正常，PID、启动时间和 revision 全程稳定。逐请求 trace 中 S1/S2/S3 的
正式 measurement 样本的 `frame -> dispatch` P50/P95/P99 分别为
`4.822/7.732/9.120 ms`、`0.622/2.107/2.561 ms`、
`1.926/4.041/4.915 ms`；handler P50/P95/P99 分别为
`0.144/0.188/0.265 ms`、`0.498/0.587/0.773 ms`、
`0.022/0.029/0.041 ms`。其中 S1 剩余固定开销包含每个 fresh 请求的 session
全局登记、三次无竞争锁访问，以及不可变 binding 的重复线性扫描和临时 tuple 构造。

提交 `6037d09ae521c5666359519da87d37fb4464a624` 完成以下收敛：

1. `max_requests_per_connection=1` 的 reactor 使用不可伪造的本地 transient lease；
   response 构造完成即显式失效，不写共享 session dict，不经过全局 session lock；
2. transient session 仍逐请求校验 Unix principal、API/event schema 版本与最新
   authorization epoch；失效 lease、其他 gateway 的 lease 和旧 epoch 均 fail-closed；
3. 不可变 binding snapshot 在构造时预建 consumer/account/strategy 只读索引；热路径
   只读查表，仍保持原有“首个有效 binding”语义；
4. persistent profile 继续使用原有共享 session 生命周期；UID/GID、wire schema、
   Journal-before-receipt、有界 lane、回放签名和 Adapter 隔离语义均未改变。

本地同进程单请求会话微基准中，`open + ListAccounts + close` 从约 `5.809 us/op`
下降到 `5.049 us/op`（约 -13.1%）。本地完整回归为
`1069 passed, 37 skipped, 308 subtests passed`，Ruff/Pyright/diff 检查通过；Linux
真实 AF_UNIX 受影响集合 `21/21` 通过。

服务器通过 Git fast-forward 与本地 editable wheel 安装部署。部署后只读
`ListAccounts=COMPLETED`；独立 1 秒预热后的 5 秒、2000 offered/s、fresh-per-request
诊断结果为 `10000/10000 ACCEPTED`、零错误，整轮墙钟 `4.999958 s`，完成吞吐
`2000.017 TPS`，端到端 P50/P95/P99/P100 为
`0.246/0.383/0.621/2.263 ms`。本轮及一次报告脚本收尾失败但请求已完整执行的前置轮
合计 `accepted=served=submitted=written=20001`，零 overload、零 lane reject、trace
零 drop，最终 active/pending 均为 0；报告脚本错误不计入性能样本。

当前定档：本次 TradeCore 代码、回归与部署专项验收通过，且单流低负载基线继续改善；
正式 S5 仍需联调方在新 revision 上独立 reset 后重跑 R1。必须同时满足三流 generator
gate、吞吐、延迟、checkpoint、完整 trace 与服务稳定性，方可签发 R1 PASS 并继续
R2/R3。
