# 真实 UDS Consumer V2 压测与验收标准

> 状态：`SERVER_TARGET_DEPLOYED_2K_CAPACITY_ACCEPTED`；当前部署 2,000 profile 的三轮证据见 [2026-10-19 验收报告](./20-Consumer-UDS-2000TPS-QPS三轮验收报告-2026-10-19.md)。
> 新增长运行高低并发混合profile：`PLANNED / PENDING_IMPLEMENTATION_AND_ACCEPTANCE`，以[长运行方案10.5节](./21-Consumer-UDS长运行优化实施与验收方案.md#105-低并发高并发共存与反复切换新增强制门禁)为准；原三轮通过不自动覆盖该新增合同。
> 适用：`ConsumerWireContractV2`、`BenchmarkStandardV1.1-Frozen`、Linux 同机策略 Consumer 数据面。  
> 安全边界：本标准不启动 Adapter、EMT、QMT 或柜台；不解除 `DISARMED`，不形成 `READY` 或场地 SLA 结论。

## 当前部署的容量目标（2026-10-19）

当前部署的正式目标为持续 **2,000 SubmitSignal TPS + 2,000 Snapshot QPS**，同时保留 **2,000 checkpointed event/s** 的消费负载；每轮三类流量共同运行 30 秒，连续执行三轮。历史 5,000 负载及 5,000 突发结果作为冗余容量证据保存，不再作为本部署 2,000 目标的必过门禁。其他项目或较高容量 profile 仍可按原 S1/S2/S3 目标单独验收。

本部署另须执行两类独立的突发测试：**2,000 次 SubmitSignal 同时释放**及 **2,000 次 ReadOrderSnapshotPage 同时释放**。每批从共同释放点到最后一个消费者校验/decode 响应完成的墙钟须小于 1,000 ms。每个请求均通过真实 UDS 接口，不平滑发送；共同释放发生在建连之前，因此批次包含连接建立与响应等待。查询固定读取已发布快照的一页，每页 100 条、快照基数至少 200,000 条；该 QPS 不能解释为每次返回全部 200,000 条订单。

当前工具的 `batch_wall_ms` 到全部任务返回为止，还包含连接关闭，采用更保守的时间上界。快照构建/发布在 release 之前完成，耗时作为 `snapshot_preparation_ms` 单独记录。

| 验收项 | 本部署目标 |
|---|---|
| 持续提交 | 每轮 60,000/60,000 ACCEPTED，零错误、零 generator release error |
| 持续快照查询 | 每轮 60,000/60,000 COMPLETED，零错误、零 generator release error |
| 持续事件消费 | 每轮 60,000 个唯一事件完成消费及持久 checkpoint，零重复、零错误 |
| SubmitSignal 延时 | P95 ≤20 ms、P99 ≤60 ms；最终 drain ≤60 ms |
| Snapshot 与 checkpoint 延时 | P95 ≤10 ms、P99 ≤40 ms；最终 drain ≤40 ms |
| 发生器健康 | 每类 offered→call-start P99 ≤10 ms，样本覆盖完整 |
| 2,000 提交突发 | 全接纳、唯一 LSN=2,000、整批 <1,000 ms |
| 2,000 查询突发 | 全部完成且每页 100 条、整批 <1,000 ms |
| HOT-1 | 10,000 次预热 + 10,000 次单在途正式样本，报告 P50/P95/P99/P99.9/P100 |
| 过载与恢复 | 保留 16,384 突发，验证有界拒绝、零 OOM/重启/trace drop、轮后队列归零及服务恢复 |
| 长运行高低并发混合（新增，待实现/验收） | 同入口低频与高频共存、0/1/10/20→2K→低压切换、背景叠加2,000突发及维护交叉；阶段/消费者独立统计，按长运行方案10.5逐项验收 |

混合负载的目标计数按声明的 `target_rates` 计算，延时、唯一性、背压、发生器健康和资源门禁维持原标准。不得把“2,000 个请求全部成功”单独写成“持续 2,000 TPS/QPS 通过”；持续负载和突发必须分别报告，三轮原始证据均保留。

运行本部署 profile 时，为 `tools/performance/run_consumer_uds_firstframe_gates.sh` 明确设置：

```bash
S1_TPS=2000 S2_EPS=2000 S3_QPS=2000 \
BURST_COUNT=2000 BURST_LIMIT_MS=1000 QUERY_BURST=1 \
S3_PROCESSES=10 S3_WORKERS=80 S3_QUEUE_CAPACITY=200 \
bash tools/performance/run_consumer_uds_firstframe_gates.sh
```

其余 `OUTPUT_ROOT`、`TRADING_DAY`、两端冻结 revision、CPU 隔离、服务受控 reset 和运维权限仍须按运行脚本及部署交接文档配置。

## 1. 目的、范围与已有结果的边界

同事的 fixture 测试已验证：

```text
模拟策略 → Consumer V2 SubmitSignal → FixtureTransport
→ GatewayApplicationService → OMS operation port → OMS journal
```

它没有建立真实 socket，因而不含 `connect`、内核 socket 缓冲、完整 frame 组装、`SO_PEERCRED`、跨进程调度、UDS 背压或客户端/服务端序列化往返。其约 `14 ms` 结果只能作为 **fixture/OMS journal 探索性基线**，不能标为 Consumer UDS 性能。

本标准的唯一被测链路是：

```text
独立策略压测进程
  → AF_UNIX/SOCK_STREAM client
  → ConsumerUnixSocketListenerV2
  → SO_PEERCRED principal binding
  → XTC2 decode + schema + binding validation
  → GatewayApplicationService / OMS operation port
  → durable OMS journal
  → IntentReceipt response frame
  → 独立策略压测进程
```

截至当前 revision，服务器 target 已以独立
`xuanling-tradecore-consumer-uds.service` 部署。它不是原有的
`xuanling-tradecore.service`，也不启动 Adapter。正式容量证据仍必须由独立
generator 在**同一 Linux 主机**上按本标准产生；WSL 自建 listener、fixture 或函数直调
均不能替代该 target。

本轮止于 `journal_first=true` 的 `IntentReceipt`，并强制 `gateway_dispatched=false`。它不证明 EMS 调度、EMT/QMT 原生调用、柜台受理、callback、成交、持仓或 `PollEvents` 的外部真实回报。真实 `UDS → OMS → EMS → EMT → callback → PollEvents` 必须以独立的低频模拟盘组合 profile 验收。

## 2. 计时模型

时间戳使用同一 Linux 主机可比较的 `CLOCK_MONOTONIC`/`time.monotonic_ns()`，以整数 ns 保存、报告显示 ms。`sample_seq` 和 `correlation_id` 只用于本轮关联；evidence 不得含账户、标的、价格、订单号、策略业务键、socket 路径或凭据。

| 点位 | 事实边界 | 记录位置 |
| --- | --- | --- |
| `U0` | client 完成 request frame 构造，即将 `sendall` | generator/client |
| `U0A` | client `sendall` 返回 | generator/client |
| `U1` | listener 收到该 frame 首字节 | listener |
| `U2` | listener 收齐并校验固定 header/长度，获得完整 XTC2 frame | listener；等同架构 `I0` |
| `U3` | decode、schema、session、`SO_PEERCRED` principal 与 account binding 全部通过 | gateway |
| `U4` | OMS intent durable journal commit 完成，`IntentReceipt` 建立 | OMS operation |
| `U5` | response 已编码且 server `sendall` 返回 | listener |
| `U6` | client 收齐并校验/decode matching response | generator/client |
| `U7` | client 已校验 `PollEvents`/snapshot response | 查询或消费 profile |

每轮必须报告下列独立区间：

```text
client send block:       U0 → U0A
request assembly:        U1 → U2
server validation:       U2 → U3
OMS durable commit:      U3 → U4
server response write:   U4 → U5
server handling:         U2 → U5
client UDS round-trip:   U0 → U6
```

`U0→U6` 是同机调用方体验，`U2→U5` 是服务端处理时间。两者不可相加，也不可与执行系统 `G0→G4`、`G5→G7` 相加或改名为 EMT/QMT 下单延时。`U1→U2` 必须单报，以识别分片、发送阻塞或 socket 背压。

## 3. 前置门禁 `P0–P7`

任一门禁失败，该轮标记 `BLOCKED`，不产生通过的性能结论。

| Gate | 必须证据 |
| --- | --- |
| `P0-REVISION` | Git revision、工作区干净、Consumer V2 schema/golden vector hash、驱动 hash。 |
| `P1-LINUX` | Linux、CPython 3.10 `.venv`、`socket.SO_PEERCRED` 可用。Windows/Python 3.12 只可做开发探索。 |
| `P2-UDS-OWNERSHIP` | socket 父目录归服务账户所有、非 symlink、权限不宽于 `0700`；socket 为 `0600`；没有未验证旧 socket。 |
| `P3-IDENTITY` | 用实际 peer UID 得到 Consumer principal 和允许 binding；错误 UID、缺 binding、跨 binding 均被拒绝。 |
| `P4-NO-ADAPTER` | 每样本 `gateway_dispatched=false`，Adapter 调用数=0，EMT/QMT process count=0。 |
| `P5-CLOCK` | 时钟源、跨进程可比性、打点开销和偏差均记录；超出 profile 偏差限制，只能报告进程内区间。 |
| `P6-OBSERVABILITY` | 原始时间对、HDR、错误分类、queue/socket 高水位、CPU/GC/上下文切换/page fault 和 evidence writer 可用。 |
| `P7-CLEAN-STATE` | 每轮使用独立 Journal namespace/database 或已验证 reset，无遗留 test intent。 |

必须回归验证的拒绝路径：错误 UID、缺/错 `consumer_account_id` binding、CRC 篡改、超长 frame、半帧、未知 operation、非法 schema。任一拒绝路径创建 OMS intent 或 Journal event 即为 `FAIL_ADMISSION_INTEGRITY`。

## 4. 不可变 Profile 与数据合同

每轮先写入脱敏 `profile.json`；修改任一字段即为新 profile，不能与旧样本比较或池化。

```json
{
  "profile_id": "TC-CONSUMER-UDS-V2-PATH-1.0",
  "measurement_schema_version": "uds-trace-v1",
  "transport": "AF_UNIX/SOCK_STREAM",
  "wire_contract": "ConsumerWireContractV2",
  "python": "CPython 3.10",
  "connection_mode": "fresh-per-request | persistent-per-vu",
  "durable_fence": "<actual OMS journal commit semantics>",
  "adapter_mode": "disabled-no-dispatch",
  "payload_class": "minimal-valid-submit-v1",
  "warmup_requests": 10000,
  "measurement_samples": 10000,
  "statistics": "nearest-rank",
  "clock": "CLOCK_MONOTONIC",
  "evidence_redacted": true
}
```

`fresh-per-request` 与 `persistent-per-vu` 是两个 profile。前者包含 `connect/close`；后者只测已建立连接上的 request/response。当前 `ConsumerUnixClientV2.call()` 每次建连，若要测 persistent profile，必须新增复用同一 `wire_v2.py` 契约的长连接 benchmark client，不能用函数直调替代。

请求使用最小、合法、固定大小 canonical payload。每个新请求必须有唯一、不可逆脱敏的业务键派生值，避免幂等命中被误计为新 Journal 写入。幂等作为独立组：相同键/相同 intent 必须只创建一个 Journal intent；同键/不同 intent 必须为 `IDEMPOTENCY_CONFLICT`。查询/事件种子必须在窗口前完成，且 seed 写入不计入消费 TPS。

预热至少 `10,000` 个成功请求，且必须在测量前完成 listener、socket bind、UID map、Journal/schema/WAL 打开、对象池和缓冲预分配。冷启动另建 `UDS-COLD-1` 报告，绝不混入热态分位数。

## 5. 必须实现的插桩与压测器

插桩不得把同步文件 I/O、日志格式化、指标上报或锁竞争放入 Consumer/OMS 热路径。生产路径只写预分配、可关闭的 binary/in-memory trace sink；窗口结束后由压测器批量落盘。

| 文件/组件 | 必须职责 |
| --- | --- |
| `xuanling_tradecore/consumer/trace_v2.py` | `ConsumerUdsTraceV1`、固定字段、单调 ns、采样开关、trace drop 计数。 |
| `xuanling_tradecore/consumer/unix_v2.py` | `U1/U2/U5`、frame assembly、连接/队列高水位；不记录业务 payload。 |
| `xuanling_tradecore/consumer/gateway_v2.py` 或 operation adapter | `U3` 与 `correlation_id/sample_seq` 关联。 |
| `xuanling_tradecore/consumer/oms_operations_v2.py` | 仅在真实 durable commit 后记录 `U4`。 |
| `xuanling_tradecore/consumer/client_v2.py` | `U0/U0A/U6`；长连接压测 client 必须复用 wire contract。 |
| `tools/consumer_uds_load_platform/` | 独立 server runner、独立 generator、HDR 汇总、资源采集、evidence 完整性校验。 |
| `tests/test_consumer_uds_benchmark_contract.py` | U 点顺序、拒绝路径零 Journal、trace 覆盖和结果 schema。 |

每个成功样本必须恰有一组 `U0,U0A,U1,U2,U3,U4,U5,U6`，严格单调。缺点、重复关联、负区间或 trace drop 让全轮 `FAIL_OBSERVABILITY_GAP`；禁止删除异常样本重算。

## 6. 工作负载、阈值与通过语义

分位数采用统一标准 nearest-rank：`rank(q)=ceil(q×n)`，`P100=max`。每个 profile 独立执行三轮，报告逐轮值、中位轮、最小/最大范围；禁止跨轮池化。

| 编号 | 目的 | 负载 | 最低样本/时长 | 通过条件 |
| --- | --- | --- | --- | --- |
| `UDS-C0` | wire/安全门禁 | P0–P7 和正反例 | 每 revision | 全部通过，Adapter dispatch=0。 |
| `UDS-HOT-1` | 单笔体验 | 1 在途，串行 SubmitSignal | 10,000 成功样本 | U0–U6 全覆盖、零失败/trace gap/错误 Journal；固定多 frontend、fresh-per-request 合同 P95≤1.50 ms、P99≤3.00 ms，P99.9/P100强制报告。 |
| `UDS-S1-2K` | 策略 signal 容量 | 2,000 accepted TPS × 30 s | ≥60,000 成功请求 | 对应 `STR-S1`；零 dropped/failure/重复 Journal，P95≤20 ms、P99≤60 ms。 |
| `UDS-S3-5K` | snapshot 查询 | 5,000 completed QPS × 30 s | ≥150,000 响应 | 对应 `STR-S3`；generation/watermark 合法，零失败，P95≤10 ms、P99≤40 ms。 |
| `UDS-S2-5K` | PollEvents 消费 | 5,000 checkpointed event TPS × 30 s | ≥150,000 events | 对应 `STR-S2`；预置可重放测试 Journal，零 gap/乱序/未解释重复，P95≤10 ms、P99≤40 ms；明确标 `synthetic event source`。 |
| `UDS-S5-MIX` | 策略混合 | S1 2K + S2 5K + S3 5K，30 s | 三流均达标 | 三流完整，零 dropped/failure，Adapter dispatch 仍为 0。 |
| `UDS-BP-1` | 背压 | 超过稳定容量或制造慢 client | 至少首个失稳档 | 有界拒绝/关闭/超时；无无界内存、错误成功或 Journal ghost intent。 |

`UDS-HOT-1` 可分 10 个独立 1,000 请求子轮，必须保留全部子轮。若未达到 2,000 TPS/5,000 QPS，不得降低目标后写“通过”；报告最高稳定档和首个失稳档。

HOT 阈值只适用于固定多 frontend 隔离拓扑的单在途、fresh-per-request 公共 UDS 调用。该阈值由同 revision 三个全新进程、generation 与 Journal namespace 的独立 10,000 样本轮次冻结；不替代 LOW、HIGH、故障、资源、投影闭合或 Adapter 柜台门禁，也不得用于把并发排队延时解释为 HOT 通过。

## 7. Generator、连接与资源规则

1. listener、generator、资源采集器必须是独立进程，不能与 server 共享事件循环或采用 in-process transport。
2. 持续压测须记录 VU、连接数、每连接 outstanding 上限、offered/sent/completed/rejected/timed-out 与 generator dropped；不能用 2,000 个 Python 线程同步 `sendall` 伪造 2,000 TPS。
3. 连续 profile 用稳定速率调度，吞吐分母为窗口内 client 收到完整、协议有效 response 的数量；不得用窗口外排空量充数。
4. `PollEvents` 使用 client 已持久化 cursor；断线、慢消费或 cursor 错误以结构化错误计数，不能悄然从 `BEGIN` 重读。

`UDS-S2-5K` 的多 worker 并行回放使用服务端 `CreateReplayPartitions` 签发的独立有界
cursor；接口、数据准备和 checkpoint 规则见 [签名分片联调说明](14-PollEvents签名分片游标与UDS-S2-5K联调说明.md)。
每个分片由一个 worker 独占，分片内位置递增，跨分片检查区间互不重叠和全体事件覆盖；
跨 worker 响应到达顺序不等于全局事件顺序。计数以处理结果和 next_cursor 已原子持久化
后的唯一事件数为准，不能把 RPC 次数、重复回放或仅收到的事件数作为 checkpointed TPS。
5. socket buffer、`RLIMIT_NOFILE`、CPU affinity、GC、CPU governor、磁盘/数据库位置与安全软件状态必须写入 profile。变更即新 profile。

每轮最少按 1 Hz 记录：每进程 CPU 时间、迁核、voluntary/involuntary context switch、GC、RSS、minor/major fault、磁盘 IOPS/延时、IRQ/softirq、accept/active connection、收发字节、socket 错误/超时、queue 高水位、`U1→U2` 和 `U3→U4` HDR。

如果 `U3→U4` 主导，必须再拆出 canonicalization、幂等索引、Journal append/事务、commit 与 receipt 生成；禁止只凭总耗时断言“SQLite/Journal 慢”。初始化/WAL 建表成本只能归入 `UDS-COLD-1`。

## 8. Evidence 与结果格式

每次运行建立新的不可覆盖目录：

```text
artifacts/consumer-uds/<profile-id>/<UTC-run-id>/
├── manifest.json
├── preflight.json
├── summary.json
├── samples.ndjson
├── hdr/
├── resources.jsonl
├── errors.ndjson
└── sha256sums.txt
```

`samples.ndjson` 最小记录如下；所有值已脱敏：

```json
{
  "sample_seq": 42,
  "profile_id": "TC-CONSUMER-UDS-V2-PATH-1.0",
  "connection_mode": "fresh-per-request",
  "result": "ACCEPTED",
  "gateway_dispatched": false,
  "trace": {"u0_ns": 0, "u0a_ns": 0, "u1_ns": 0, "u2_ns": 0, "u3_ns": 0, "u4_ns": 0, "u5_ns": 0, "u6_ns": 0}
}
```

`summary.json` 必须输出每区间 `count/P50/P95/P99/P100`、有效样本≥10,000 时的 P99.9、吞吐、整轮墙钟、三轮范围、VU/连接、成功/拒绝/超时/错误、trace 覆盖、generator drop、资源高水位和失败原因。

## 9. 失败分类与禁止外推

| 情况 | 正确状态 |
| --- | --- |
| FixtureTransport/函数直调/loopback TCP | `NON_QUALIFYING_FIXTURE`。 |
| 非 Linux 或无 `SO_PEERCRED` | `BLOCKED_TRANSPORT_IDENTITY`。 |
| 成功请求有 `gateway_dispatched=true` | `FAIL_UNEXPECTED_ADAPTER_SIDE_EFFECT`，停止压测。 |
| trace 缺失/关联错误 | `FAIL_OBSERVABILITY_GAP`。 |
| 拒绝输入写入 OMS Journal | `FAIL_ADMISSION_INTEGRITY`。 |
| 吞吐门槛未达 | 仅报告稳定上限/首失稳档，不得写通过。 |

禁止：测量中动态安装依赖、建表/懒启动、改变 binding、同步业务日志、输出原始 payload、使用账户密码、启动 Adapter、删除坏样本、混合连接模式/revision 的时间对。

## 10. 实施、复核与交付顺序

1. **T0 插桩**：完成 U0–U6 trace、样本完整性检查及 fixture 非合格标识；离线 contract tests 通过。
2. **T1 UDS smoke**：真实 Linux listener/client，验证 socket 权限、`SO_PEERCRED`、一笔成功和全部拒绝路径，证明 Adapter=0。
3. **T2 热态**：完成 `UDS-HOT-1` 三轮；先按 UDS/校验/commit 子阶段归因。
4. **T3 容量**：依次运行 `UDS-S1-2K`、`UDS-S3-5K`、`UDS-S2-5K`、`UDS-S5-MIX` 与 `UDS-BP-1`，每项独立 reset、三轮运行。
5. **T4 独立复核**：另一位开发者仅凭 manifest、profile、driver hash 与脱敏 evidence 重跑，复核 schema、计数、trace 完整性和 Adapter=0。
6. **T5 归档**：报告放在本目录，明确已通过 profile；本轮之外的 EMS/EMT 组合仍维持 `CODE_COMPLETE_PENDING_ACCEPTANCE`。

## 11. 最终验收清单

- [ ] 真正使用 `AF_UNIX/SOCK_STREAM`，没有 FixtureTransport 或函数直调。
- [ ] Linux `SO_PEERCRED`、目录/socket 权限和错误 UID 拒绝均有证据。
- [ ] 每个成功样本 U0–U6 完整、单调、零 trace drop。
- [ ] `U4` 绑定真实 durable Journal commit，而非内存对象创建。
- [ ] 所有正常样本 `gateway_dispatched=false`；所有拒绝路径零 Journal intent。
- [ ] 热态、2K TPS、5K QPS、5K event TPS、混合、背压均通过，或如实报告阻断。
- [ ] 三轮 evidence、HDR、资源数据、SHA-256 和脱敏原始时间对齐全。
- [ ] 全量测试在独立可写 runtime 根目录通过；不得遗留权限错误。
- [ ] 报告没有把 UDS 基线外推为 EMS、EMT、QMT、柜台或成交性能。
