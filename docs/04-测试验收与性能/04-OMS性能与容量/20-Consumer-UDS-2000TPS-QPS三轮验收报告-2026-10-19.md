# Consumer UDS 2,000 TPS/QPS 三轮验收报告（2026-10-19）

## 1. 验收结论

**当前部署在本报告声明的 Consumer UDS profile 下三轮全部通过。**

- 每轮持续 30 秒同时运行 `2,000 SubmitSignal TPS + 2,000 Snapshot QPS + 2,000 checkpointed event/s`，三轮每类均完成 60,000 个目标，全部在测量窗口内完成；零业务错误、零 release error、零最终 drain。
- 2,000 次 SubmitSignal 同时释放：三轮均全部 ACCEPTED、唯一 LSN=2,000，整批 `420.899–431.317 ms`。
- 2,000 次 ReadOrderSnapshotPage 同时释放：三轮均全部 COMPLETED、每页 100 条，整批 `416.781–433.164 ms`。
- 16,384 过载三轮均为 `8,192 ACCEPTED + 8,192 ConnectionResetError`，背压保持有界。
- trace 全覆盖、零丢失；没有 OOM、内存限额事件或服务重启；最终受控 reset 后服务 active，单笔恢复探针通过。

本部署的正式容量目标已按用户要求调整为 2,000 TPS/QPS。此前 5,000 测试保留为冗余容量证据，不再以 5,000 整批小于 1 秒作为本部署的发布容量门槛；其历史失败结论仍保留原始含义，不改写历史结果。

本轮使用真实 Linux UDS，但 Adapter 为 `disabled-no-dispatch`。通过结论属于本地 Consumer→OMS 接纳/查询/消费链路，不代表 EMT 柜台达到 2,000 TPS，也不解除交易恢复的其他发布门禁。

## 2. 冻结对象与测量合同

| 项目 | 固定值 |
|---|---|
| 服务版本 | `dev@dbc85b8150cebf1077bf1bcdd7999157208d63d2` |
| 压测工具 | `test/isolated-generator-affinity@228c2dbee470dbe16c89df0459f7e8d9f2205328` |
| 服务器 | `192.168.251.1` |
| 服务 checkout | `/opt/xuanling-tradecore` |
| 发生器 checkout | `/home/xuanling/tradecore-loadgen-72288c4` |
| 服务 CPU | `64-79,160-175` |
| 发生器 CPU | `32-40,48-59,80-95`；与服务 CPU 不重叠 |
| 服务内存上限 | `6 GiB` |
| 发生器内存上限 | `3 GiB` |
| 接口 | `AF_UNIX/SOCK_STREAM`、Consumer V2、XTC2、fresh-per-request |
| Journal/reset | 每轮服务受控重启，创建新 namespace，旧证据保留 |
| 冻结快照与 replay 基数 | 每轮通过公开 SubmitSignal 建立 200,000 条；HOT 20,000 + seed 180,000 |
| 查询粒度 | 已发布的不可变快照，每页 100 条；不是每次查询完整订单集 |
| 统计 | 原始 monotonic ns 时间戳，nearest-rank 分位数 |
| 正式轮次 | R1、R2、R3；全部使用同一组 revision 和目标参数 |

发生器只负责调用公开 Consumer 接口和读取结果。运维编排负责 reset、资源采集和 trace 归档。服务代码保持此前已部署版本；本轮修改压测工具的目标参数、查询突发和结果判定，并固化部署的 2,000 容量合同。

S1 使用 4 个发生器进程、64 个 worker；S3 使用 10 个进程、80 个 worker、有界 release queue 总容量 200。S2 每次 PollEvents 最多 100 条，目标 2,000 event/s 对应每秒 20 次 PollEvents 请求，包含消费者本地持久 checkpoint。三类负载共约 4,020 次 UDS 请求/秒，不把 2,000 event/s 误写成 2,000 PollEvents QPS。

## 3. 持续混合负载

三轮每类 offered 和完成目标均为 60,000，S2 unique events 也均为 60,000。所有请求/事件均在各自 30 秒测量窗口内完成，`drain_ms=0`；错误和 release error 全部为零。

### 3.1 消费者体验延时

单位：ms。S1/S3 从计划释放到调用结果校验完成；S2 到本地 checkpoint 持久化完成。

| 轮次 | 操作 | 完成速率 | P50 | P95 | P99 | P100 |
|---|---|---:|---:|---:|---:|---:|
| R1 | SubmitSignal | 2,000 TPS | 3.187696 | 5.807110 | 6.332948 | 10.160620 |
| R1 | Snapshot page | 2,000 QPS | 3.895300 | 5.525685 | 5.902465 | 8.328955 |
| R1 | checkpoint event | 2,000 event/s | 4.979616 | 6.111837 | 6.918580 | 8.541627 |
| R2 | SubmitSignal | 2,000 TPS | 3.366728 | 5.933452 | 6.487561 | 22.484243 |
| R2 | Snapshot page | 2,000 QPS | 3.909380 | 5.558357 | 5.998208 | 17.276978 |
| R2 | checkpoint event | 2,000 event/s | 4.822026 | 5.682341 | 7.075823 | 21.996564 |
| R3 | SubmitSignal | 2,000 TPS | 3.228115 | 5.862250 | 6.757078 | 30.484772 |
| R3 | Snapshot page | 2,000 QPS | 3.929430 | 5.565529 | 6.227696 | 27.014824 |
| R3 | checkpoint event | 2,000 event/s | 5.436749 | 6.644792 | 10.136619 | 21.701005 |

沿用原延时门槛：S1 P95≤20 ms/P99≤60 ms，S2/S3 P95≤10 ms/P99≤40 ms。三轮全部满足。

### 3.2 发生器排队

| 轮次 | S1 offered→call-start P99 ms | S2 P99 ms | S3 P99 ms |
|---|---:|---:|---:|
| R1 | 0.139962 | 0.048826 | 1.889781 |
| R2 | 0.146156 | 0.053175 | 1.903800 |
| R3 | 0.139195 | 0.061374 | 1.905620 |

均小于 10 ms，call-start 样本覆盖完整，发生器没有通过无界排队或窗口后的补发伪造吞吐。

### 3.3 同窗服务端 accepted→send-finished

按每轮 `common_start_ns` 到 `+30 s` 过滤原始服务端 trace。S1/S3 各 60,000 条，PollEvents 各 600 次请求，覆盖与消费者计数一致。

| 轮次 | 操作 | P50 ms | P95 ms | P99 ms | P100 ms |
|---|---|---:|---:|---:|---:|
| R1 | SubmitSignal | 2.877985 | 5.310983 | 5.863789 | 9.283764 |
| R1 | Snapshot page | 1.654430 | 3.381140 | 3.828538 | 5.832874 |
| R1 | PollEvents | 1.149496 | 1.519160 | 1.658458 | 3.040584 |
| R2 | SubmitSignal | 2.997596 | 5.442973 | 6.081597 | 21.114404 |
| R2 | Snapshot page | 1.687998 | 3.414644 | 3.999868 | 15.243807 |
| R2 | PollEvents | 1.132656 | 1.471468 | 1.621519 | 7.406520 |
| R3 | SubmitSignal | 2.906419 | 5.372887 | 6.319992 | 29.590331 |
| R3 | Snapshot page | 1.663857 | 3.397138 | 4.581253 | 25.334030 |
| R3 | PollEvents | 1.150766 | 1.518351 | 1.899266 | 14.645594 |

服务端区间不包含客户端 release queue、连接建立、解码及 S2 checkpoint 落盘，不能与消费者分位数相减或相加来推导另一段分位数。

## 4. 2,000 同时释放突发

所有协程先准备好 frame 并到达同一 release barrier，释放后才开始建连，随后各调用一次接口，不平滑发送、不重试失败。`batch_wall_ms` 从共同释放到全部任务结束，包括响应解码校验及连接关闭；这是比“最后一条响应 decode 完成”更保守的整批计时。

逐笔分位数从该调用开始到匹配响应解码完成，不能代替整批墙钟；共同释放到单个协程真正获得调度的等待已计入整批墙钟。

### 4.1 SubmitSignal

| 轮次 | ACCEPTED/目标 | 唯一 LSN | P50 ms | P95 ms | P99 ms | P100 ms | 整批墙钟 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| R1 | 2,000/2,000 | 2,000 | 242.771664 | 392.393927 | 406.194202 | 409.385613 | 420.898572 |
| R2 | 2,000/2,000 | 2,000 | 247.813394 | 402.173056 | 416.276850 | 419.681639 | 431.316660 |
| R3 | 2,000/2,000 | 2,000 | 243.326207 | 392.882810 | 406.510690 | 409.646830 | 421.352189 |

### 4.2 ReadOrderSnapshotPage

| 轮次 | COMPLETED/目标 | 每页条数 | P50 ms | P95 ms | P99 ms | P100 ms | 整批墙钟 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| R1 | 2,000/2,000 | 100 | 210.082644 | 355.304102 | 359.809692 | 360.789118 | 420.273138 |
| R2 | 2,000/2,000 | 100 | 207.623242 | 362.762823 | 367.370483 | 369.295622 | 433.164345 |
| R3 | 2,000/2,000 | 100 | 211.373118 | 353.840658 | 357.524672 | 358.507239 | 416.781370 |

查询前通过 CreateOrderSnapshot 准备已发布快照，等待耗时分别为 `4,710.148/4,684.741/4,703.622 ms`，在共同释放之前，单独记录为 `snapshot_preparation_ms`。这一结果证明已发布快照的分页读取能力，不证明每次请求都重新构建最新全量快照仍可达到相同 QPS。

两类突发的最慢整批为 `433.164 ms`，距 1,000 ms 预算仍余约 `566.836 ms`；整批平均排空速率约 4,617–4,799 次/秒。该排空速率只描述本次有限突发，持续能力由第 3 节单独证明。

## 5. 热态单笔

每轮 10,000 次预热 + 10,000 次单在途正式样本，全部 ACCEPTED，唯一正式 LSN=10,000。

| 轮次 | P50 ms | P95 ms | P99 ms | P99.9 ms | P100 ms |
|---|---:|---:|---:|---:|---:|
| R1 | 0.219183 | 0.239312 | 0.317527 | 0.784954 | 8.425941 |
| R2 | 0.222488 | 0.243549 | 0.353813 | 0.783958 | 0.825759 |
| R3 | 0.219505 | 0.240897 | 0.379051 | 0.798211 | 3.166478 |

低并发 P95 稳定在约 0.24 ms；P100 的单次调度抖动照实保留，不将其隐藏在平均值中。

## 6. 过载、trace 与资源

| 轮次 | 16,384 过载接纳/拒绝 | 过载墙钟 ms | trace submitted=written | 内存峰值 bytes |
|---|---:|---:|---:|---:|
| R1 | 8,192/8,192 | 2,749.369842 | 333,053 | 5,508,710,400 |
| R2 | 8,192/8,192 | 2,735.163145 | 333,050 | 5,511,909,376 |
| R3 | 8,192/8,192 | 2,810.332954 | 333,055 | 5,510,807,552 |

拒绝类型均为 ConnectionResetError，属于超出首帧容量的明确连接拒绝。它们只计入过载测试，不能混入正常 2,000 场景的成功计数。

三轮均满足：

- `preframe_capacity=8192`、`total_connection_capacity=10240`，preframe 峰值 8,192、ready/pending 峰值 512；
- `queue_dropped=capacity_dropped=0`、`writer_failed=false`；
- 轮后 active/preframe/pending connections 和 pending output bytes 全部归零，无 lane reject；
- `memory.high/max/oom/oom_kill/oom_group_kill=0`；
- `Result=success`、`NRestarts=0`。

最大内存峰值约 `5.133 GiB`，占 6 GiB 配额约 `85.6%`。该峰值包含本轮大快照、额外版本构建、Journal、trace 和过载请求，尚余约 0.867 GiB；不能据此推断无限增长数据下的长期内存安全。

最终恢复检查：服务 active、`NRestarts=0`，当次 MainPID=3870372，内存约 28 MiB，恢复单笔 Consumer 探针通过。测试结束后发生器 unit 为 `Result=success`、inactive/dead（正常完成），业务服务继续运行。

## 7. 门禁判定与发布边界

`overall.json` 的 R1/R2/R3 均为 true，五项逐轮 checks（mixed、burst_count、burst_wall、query_count、query_wall）全部为 true。运行脚本还在每轮核验 trace、资源事件、过载和队列归零，最终恢复成功。

| 项目 | 判定 |
|---|---|
| 持续 2,000 TPS + 2,000 QPS + 2,000 event/s | PASS，3/3 |
| 2,000 提交同时释放，全部接纳且 <1 s | PASS，3/3 |
| 2,000 查询同时释放，全部完成且 <1 s | PASS，3/3 |
| 低并发 HOT | PASS，3/3 |
| 有界过载、trace 完整性、无 OOM/重启 | PASS，3/3 |
| 最终服务恢复 | PASS |
| 当前 Consumer 2,000 容量 profile | **PASS** |

当前无需为了本部署 2,000 目标继续引入多进程服务分片或原生扩展。服务保留现有容量冗余；后续按真实消费者 payload、快照数据规模和长运行留存策略做独立 soak 验收。本次三轮各 30 秒的容量测试不外推为数日长运行证明。

## 8. 可复现证据与验证

- 最终证据根目录：`/data/tradecore-performance/target-2k-228c2db-20261019T071200Z`
- 每轮：`hot/summary.json`、`seed/summary.json`、`mixed/summary.json` 和分片原始数据、`burst-2000.samples.ndjson`、`query-burst.samples.ndjson`、`overload-16384.samples.ndjson`。
- 服务端：每轮 `trace.jsonl`、`trace.status.json`、`environment-before.txt`、`service-after.txt`、`memory.events.txt`。
- 汇总/恢复：`overall.json`、`recovery/summary.json`、`service-recovered.txt`。
- 完整本地工具测试：`228 passed, 2 skipped, 16 subtests passed`；随后快照准备有界等待修改的相关回归 `18 passed`；Ruff 和服务器 `bash -n` 通过。
- 工具支持显式 `--s1-tps/--s2-eps/--s3-qps`；目标写入 `target_rates`，验收按声明速率计算并保留原延时、唯一性和发生器健康门禁。
- 标准与运行参数见 [真实 UDS Consumer V2 压测与验收标准](./11-真实UDS-Consumer-V2压测与验收标准.md)。

最终证据只使用上述冻结 revision 的完整 R1–R3。此前工具预检、查询准备调试和冒烟目录均不拼接进正式三轮。
