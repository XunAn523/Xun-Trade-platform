# Consumer UDS 首帧接入改造与完整门禁报告（2026-10-19）

> 本报告保留 5,000 目标的历史实测判定。后续正式部署目标已调整为 2,000 TPS/QPS，当前三轮全部通过的证据见 [2,000 容量验收报告](./20-Consumer-UDS-2000TPS-QPS三轮验收报告-2026-10-19.md)。

## 1. 最终结论

本轮已完成 Consumer UDS 首帧接入容量/接入模型改造，并在最终冻结 revision 上完整执行三轮 `HOT-1 + S5-MIX + BURST-5000 + BP-16384 + RECOVERY` 门禁。

改造解决了原固定模型在 4,096 个首帧连接处确定性截断的问题：三轮 `BURST-5000` 均为 `5,000/5,000 ACCEPTED`，唯一 accepted LSN 为 5,000；`BP-16384` 三轮均为 `8,192 ACCEPTED + 8,192 ConnectionResetError`，与有界首帧容量完全一致。服务没有 OOM、没有重启、没有 trace 丢失，轮后连接和队列归零并恢复 active。

但本轮**不能判定全部性能门禁通过**：

- 5,000 个消费者连接共同释放的整批墙钟为 `1,221.013–1,231.861 ms`，尚未达到 `<1,000 ms` 的硬目标；
- 持续混合门禁只有 R2 全部达标，R1、R3 未通过；R3 服务端三类 handler 同时出现约 `38–40 ms` 的 P95 停顿，证据指向共享主机调度抖动，而不是单一业务 lane 缺陷；
- 因此最终 verdict 为 fail-closed：容量、安全性和恢复闭包通过，整体验收不冒充全绿。

## 2. 冻结对象与环境

| 项目 | 冻结值 |
|---|---|
| 服务端 revision | `dev@dbc85b8150cebf1077bf1bcdd7999157208d63d2` |
| 压测工具 revision | `test/isolated-generator-affinity@5c5079ea18a331488912ff8bcf7a7046c9d224d6` |
| 服务 checkout | `/opt/xuanling-tradecore` |
| 压测 checkout | `/home/xuanling/tradecore-loadgen-72288c4` |
| UDS | `/run/tradecore-consumer-uds/consumer-v2.sock` |
| 服务单元 | `xuanling-tradecore-consumer-uds.service` |
| 服务 CPU | `64-79,160-175` |
| 发生器 CPU | `32-40,48-59,80-95` |
| 正式压测内存上限 | `3 GiB` |
| Adapter | `NO_ADAPTER_BENCHMARK / disabled-no-dispatch` |
| 数据安全边界 | 不启动 EMT/QMT，不产生真实委托或撤单 |

正式发生器运行于独立 transient systemd unit，固定 `MemoryMax=3G`、`TasksMax=512`、`LimitNOFILE=65536`、`CPUWeight=10000`、`Nice=-5`。压测客户端只调用公开 Consumer V2 接口；服务 reset、trace 关闭和资源证据归档由运维编排完成。

## 3. 首帧接入模型改造

### 3.1 原模型问题

旧实现虽然配置了更大的连接容量，但 accept 行为被 `decoded-ready=24` 间接限制：当少量已解码连接等待 reactor dispatch 时，监听器停止继续 drain kernel accept queue。5,000 连接同时发送首帧时，实际只能稳定接纳 4,096 个连接，其余 904 个在响应前被 reset。

### 3.2 最终模型

最终实现将“连接接入”和“已解码请求调度”解耦：

1. listener 持续把已建立连接接入到有界 preframe 状态，不再因低负载 decoded-ready 水位而停止 accept；
2. 保留低负载快速路径：ready 上限 24、dispatch 24、等待上限 2 ms，不牺牲热态单笔；
3. 当 pending/ready 达到 burst 水位 512 时，切换到 burst 模式：ready 上限 512、单次 dispatch 128、等待上限 25 ms；
4. preframe 总容量固定为 8,192，超额连接明确 reset，不允许无界增长；
5. trace 持续记录 preframe/pending 峰值、overload、writer drop 和关闭时残留，用于证明接入容量和恢复闭包。

### 3.3 A/B 选择

| revision/方案 | HOT P50/P95/P99 ms | 5,000 整批墙钟 ms | 16,384 过载墙钟 ms | 结论 |
|---|---:|---:|---:|---|
| 旧基线 `c90a090` | — | 最快 `1,681` | — | 5,000 路径较慢 |
| 首版自适应 `4366681` | `0.226/0.258/0.389` | `1,410` | `3,300` | 改善 |
| 最终参数 `f95e746` 等价状态 | `0.224/0.255/0.382` | `1,202` | `2,995` | 最优，保留 |
| 激进参数 `e857d3e` | `0.228/0.254/0.371` | `1,281` | `2,858` | 5,000 退化，已 revert |

最终冻结 revision `dbc85b8` 等价于 `f95e746` 的运行参数，并包含对激进参数的显式回退。相对旧基线最快 `1,681 ms`，快速 A/B 中 5,000 突发缩短约 `28.5%`。

## 4. 代码级验证

### 4.1 服务端

- 完整测试：`1,073 passed, 38 skipped, 310 subtests`；
- Ruff：通过；
- Pyright：通过；
- 最终参数定向回归：`6 passed, 11 skipped`；
- 新增回归覆盖：reactor 热循环不再逐秒同步写 INFO 日志；低负载和 burst 两套水位/dispatch 行为；有界 preframe 接入。

### 4.2 压测工具

- 测试：`225 passed, 2 skipped, 16 subtests`；
- Ruff：通过；
- Pyright：通过；
- S1 使用 4 个发生器进程，S3 使用 25 个发生器进程，避免单个 Python load generator 把客户端瓶颈误判为服务端瓶颈；
- 汇总证据保留全量原始样本、唯一 LSN、release error、队列延时和服务端逐操作 trace；
- runner 不再首轮失败即中止，保证 R1–R3 所有门禁和最终恢复都被执行。

## 5. 三轮正式结果

### 5.1 R1

| 场景/指标 | 结果 |
|---|---|
| HOT P50/P95/P99/P99.9/P100 ms | `0.226090 / 0.254142 / 0.380192 / 0.870200 / 1.879707` |
| S1 SubmitSignal | `59,903/60,000`，release error `97`；P50/P95/P99/P100 `4.236/10.066/93.721/128.606 ms` |
| S2 PollEvents | `150,000/150,000`，无错误；P50/P95/P99/P100 `5.798/12.517/38.753/73.236 ms` |
| S3 Snapshot | `150,000/150,000`，无错误；P50/P95/P99/P100 `6.868/14.954/76.626/111.288 ms` |
| BURST-5000 | `5,000/5,000 ACCEPTED`，唯一 LSN 5,000；整批 `1,221.013 ms` |
| BP-16384 | `8,192 ACCEPTED + 8,192 reset`；整批 `2,966.131 ms` |
| 判定 | **FAIL**：混合性能不达标；容量/背压/安全闭包通过 |

服务端 accepted→sent：Snapshot P50/P95/P99/P100 为 `4.649/13.117/33.883/77.496 ms`；SubmitSignal 为 `3.899/8.802/32.924/73.437 ms`；PollEvents 为 `1.593/5.628/33.591/68.032 ms`。

### 5.2 R2

| 场景/指标 | 结果 |
|---|---|
| HOT P50/P95/P99/P99.9/P100 ms | `0.224741 / 0.253673 / 0.378546 / 0.881436 / 1.265031` |
| S1 SubmitSignal | `60,000/60,000`，无错误；P50/P95/P99/P100 `4.111/7.220/20.113/44.412 ms` |
| S2 PollEvents | `150,000/150,000`，无错误；P50/P95/P99/P100 `5.780/7.275/19.497/40.389 ms` |
| S3 Snapshot | `150,000/150,000`，无错误；P50/P95/P99/P100 `6.735/8.981/22.881/47.151 ms` |
| BURST-5000 | `5,000/5,000 ACCEPTED`，唯一 LSN 5,000；整批 `1,231.861 ms` |
| BP-16384 | `8,192 ACCEPTED + 8,192 reset`；整批 `2,821.623 ms` |
| 判定 | **PASS**：本轮定义的混合、容量、背压和安全门禁全部通过；5,000 `<1 s` 独立硬目标仍未通过 |

服务端 accepted→sent：Snapshot P50/P95/P99/P100 为 `4.411/7.186/20.677/43.414 ms`；SubmitSignal 为 `3.783/6.752/18.110/41.546 ms`；PollEvents 为 `1.553/2.278/15.195/34.533 ms`。

### 5.3 R3

| 场景/指标 | 结果 |
|---|---|
| HOT P50/P95/P99/P99.9/P100 ms | `0.204452 / 0.234797 / 0.316838 / 0.637503 / 0.843508` |
| S1 SubmitSignal | `57,901/60,000`，release error `2,099`；P50/P95/P99/P100 `5.125/113.382/116.771/123.677 ms` |
| S2 PollEvents | `150,000/150,000`，无错误；P50/P95/P99/P100 `6.118/43.106/45.862/74.312 ms` |
| S3 Snapshot | `150,000/150,000`，无错误；P50/P95/P99/P100 `7.724/72.392/79.005/104.438 ms` |
| BURST-5000 | `5,000/5,000 ACCEPTED`，唯一 LSN 5,000；整批 `1,224.582 ms` |
| BP-16384 | `8,192 ACCEPTED + 8,192 reset`；整批 `2,936.929 ms` |
| 判定 | **FAIL**：混合性能不达标；容量/背压/安全闭包通过 |

服务端 accepted→sent 同窗出现共振式停顿：Snapshot P95/P99 `39.643/41.116 ms`、SubmitSignal `38.223/39.857 ms`、PollEvents `37.806/40.165 ms`。三条独立业务 lane 同时升高，说明本轮尾延时主要来自共享主机调度/系统抖动，不是 Snapshot、Submit 或 PollEvents 某一条路径独占阻塞。

## 6. 5,000 与过载容量结果

三轮 5,000 突发全部接纳，首帧容量缺口已经修复：

| 轮次 | ACCEPTED | Reset | 唯一 LSN | 整批墙钟 ms |
|---|---:|---:|---:|---:|
| R1 | 5,000 | 0 | 5,000 | 1,221.013 |
| R2 | 5,000 | 0 | 5,000 | 1,231.861 |
| R3 | 5,000 | 0 | 5,000 | 1,224.582 |

三轮 16,384 过载均严格停在 `8,192` 接纳边界：

| 轮次 | ACCEPTED | Reset | 期望容量匹配 | 整批墙钟 ms |
|---|---:|---:|---|---:|
| R1 | 8,192 | 8,192 | 是 | 2,966.131 |
| R2 | 8,192 | 8,192 | 是 | 2,821.623 |
| R3 | 8,192 | 8,192 | 是 | 2,936.929 |

这证明当前模型不是通过无界队列换取“全接纳”。5,000 在已声明容量内全部成功；超过 8,192 后确定性拒绝，进程不崩溃、不 OOM、不污染后续轮次。

## 7. 证据与资源闭包

| 检查项 | R1 | R2 | R3 |
|---|---:|---:|---:|
| trace submitted=written | `424,722` | `424,813` | `422,714` |
| queue/capacity trace drop | `0/0` | `0/0` | `0/0` |
| preframe 峰值 | `8,192` | `8,192` | `8,192` |
| pending 峰值 | `512` | `512` | `512` |
| 轮后 pending output | `0` | `0` | `0` |
| memory.high/max/oom/oom_kill | 全部 0 | 全部 0 | 全部 0 |

最终恢复检查确认服务单元 active/running、`NRestarts=0`；当次观测内存约 `28.7 MiB`、峰值约 `30.5 MiB`。正式 runner 因总体 verdict 失败而以 fail-closed 状态结束，但已执行最终恢复，未把服务留在停止或脏队列状态。

## 8. 门禁矩阵

| 门禁 | 结果 | 说明 |
|---|---|---|
| HOT 低并发热态 | PASS | 三轮 P95 `0.235–0.254 ms`，未因 burst 模式恶化 |
| 5,000 同时释放全部接纳 | PASS | 三轮均 5,000/5,000、唯一 LSN 5,000 |
| 5,000 整批 `<1 s` | **FAIL** | 三轮约 `1.22–1.23 s` |
| 16,384 有界过载 | PASS | 三轮均严格 `8,192 + 8,192` |
| trace 完整性 | PASS | submitted=written，零 drop |
| OOM/进程安全 | PASS | memory events 全零，服务零重启 |
| 测试后恢复 | PASS | 队列归零，服务 active |
| S5-MIX 三轮性能 | **FAIL** | 仅 R2 通过，R1/R3 受共享主机抖动影响 |
| 本轮总体 | **FAIL-CLOSED** | 不以单轮通过替代三轮稳定性 |

## 9. 后续优化边界

当前单 Python reactor 的参数调优已经出现边际收益递减：扩大 batch、提高 ready 上限和更长等待虽然缩短 16,384 过载排空，但会恶化 5,000 目标路径，因此没有保留。

若必须把 5,000 同时释放从约 1.22 秒压入 1 秒，下一步应进行数量级架构优化，而不是继续放大单 reactor 参数：

1. 多进程 ingress/journal shard，每个 shard 独立 listener/reactor/journal，通过稳定键路由；
2. 或增加批量 SubmitSignal frame/原生解析扩展，降低 5,000 次 Python 解码、对象创建和响应编码成本；
3. 保持全局有界容量、每条唯一 LSN、顺序语义、trace 和 fail-closed 背压合同不变。

若要求连续三轮 S5-MIX 稳定达标，还需提供独占 cpuset/独占测试窗口或协调共享主机的外部工作负载。当前没有为了压测擅自迁移或限制服务器其他进程；R3 的跨 lane 同步停顿因此被如实保留。

## 10. 证据位置

- 最终三轮全部门禁：`/data/tradecore-performance/final-gates-dbc85b8-20261019T040127Z`
- 旧基线完整门禁：`/data/tradecore-performance/all-gates-c90a090-20261019T033613Z`
- 首版自适应 A/B：`/data/tradecore-performance/burst-ab-4366681-20261019t035349z`
- 最终参数 A/B：`/data/tradecore-performance/burst-ab-f95e746-20261019t035651z`
- 已回退激进参数 A/B：`/data/tradecore-performance/burst-ab-e857d3e-20261019t035926z`

所有路径均为服务器端证据目录；报告不包含密码、密钥、账户号或委托内容。
