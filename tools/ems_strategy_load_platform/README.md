# EMS 与策略消费端性能平台

此目录是 [统一压测口径定档标准](../../docs/04-测试验收与性能/05-统一性能压测标准.md)
中 `E0--E5`、`S0--S5` 与 `OMS-W7-EMS-STR` 的受控负载平台。

它是独立的 loopback HTTP 进程，不是生产 EMS，也不具备下单能力。平台刻意不导入
EMT SDK、Adapter、mmap/Journal writer 或 broker endpoint；因此其通过不能替代
`EXEC-BURST` 的 EMT accepted、callback 完整性或清理证据。

## 被测实现边界

| 标准组 | 实际进入的项目代码 | 完成边界 |
| --- | --- | --- |
| E1 / E3 | `EmsControlPlaneV1`、`EmsPendingPlanBufferV1`、`AccountSessionShardRouter`、`ExecutionScheduler`、`MaxQuantitySliceAlgorithm` | plan admitted / child instruction ready / execution scope enqueued |
| E2 | EMS immutable plan view 与 execution snapshot | 查询响应构造完成 |
| E4 | EMS pause/resume/cancel/reconcile control | control applied |
| S1 | `ConsumerGatewayV2` session、协议版本、ACL、account/strategy binding，再进入 E1 | signal admitted to EMS |
| S2 | deterministic 合成 contract event、单 consumer checkpoint、去重、gap fail-closed | checkpoint/state apply 完成 |
| S3 | strategy immutable snapshot | 查询响应构造完成 |
| S4 | checkpoint restart、pause/resume | 无 gap、明确拒绝或继续 checkpoint |

S1 的 `ConsumerGatewayV2` 后端操作端口仅返回无 broker 标识的测试 receipt，随后再进入真实
EMS control-plane；它用于测量授权和入 EMS 的边界，不会生成订单。S2 事件的 `source` 固定为
`deterministic-contract-feed`，不得作为真实场地回调或 `G5--G7` 证据。

## 指标

服务端 `/perf/v1/metrics/hdr` 使用 `HDRHistogramV1`，至少输出：

```text
plan_submit_to_admitted_ns
plan_admitted_to_child_instruction_ready_ns
child_instruction_to_execution_scope_enqueued_ns
ems_plan_query_ns / ems_execution_query_ns / ems_control_submit_to_applied_ns
strategy_signal_submit_to_ems_admitted_ns
g7_to_strategy_consumer_receive_ns
strategy_consumer_receive_to_checkpoint_applied_ns
strategy_snapshot_query_ns
```

对应 HTTP 路径：

```text
POST /perf/v1/ems/plans                 E1/E3
GET  /perf/v1/ems/plans/{plan_id}       E2
GET  /perf/v1/ems/snapshot              E2
POST /perf/v1/strategy/signals          S1
POST /perf/v1/strategy/events           S2
GET  /perf/v1/strategy/snapshot         S3
POST /perf/v1/admin/{ems|strategy}/...  E4/S4，仅临时 fault key 开启时存在
```

## 受控运行

先启动一个或多个隔离 shard；多个 URL 时，k6 在 shard 间分配请求，S2 同一 VU 始终落在同一
shard，以保留 consumer checkpoint 顺序。

```bash
PYTHONPATH=. .venv/bin/python -m tools.ems_strategy_load_platform.server \
  --host 127.0.0.1 --port 18180 --scopes 1 --enable-faults --fault-key "$FAULT_KEY"

PYTHONPATH=. .venv/bin/python -m tools.ems_strategy_load_platform.run_e0_s5 \
  --base-url http://127.0.0.1:18180 \
  --k6-bin /path/to/k6 --fault-key "$FAULT_KEY" \
  --output-dir artifacts/performance/ems-strategy/<run-id>
```

默认每个 E/S steady workload 持续 30 秒。E4/S4 的独立故障演练会在同一命令结尾运行。
运行结论只有在所有 k6 gate、HDR schema、E4/S4 drill 都通过时才能是 `PASS`。原始输出为
可重跑测试工件，放入 `artifacts/performance/`，不入 Git。

正式 Linux 运行必须把每个 sidecar 的 `RLIMIT_NOFILE` 提升至至少 `8192`，并通过 health 中的
`open_file_soft_limit` 由 runner 预检；例如在启动命令外层使用 `ulimit -n 8192 && exec ...`。
这是每个受测 sidecar 的局部资源边界，不修改主机全局限制，也不影响其他进程。Windows 本地冒烟可在
runner 上显式传 `--minimum-nofile 0`，但不能作为统一容量验收证据。

`run_w7_ems_str.py` 只接收既有、已审核的 OMS+EMT W7 argv JSON；它不会自行构造 EMT
命令。调用时还必须显式提供 `--oms-base-url`：驱动确认每个 OMS shard 已实际接纳 W3 fact 后，
才启动 E/S k6，使两者处于同一 60 秒窗口，并分别报告两个退出状态。真实 EMT 的 preflight、
499 accepted、callback pair 与 cleanup 仍完全由既有 W7 驱动裁定。正式 Linux 运行还须提供
`--ems-k6-cpu-list`，并确保它与 OMS sidecar、OMS k6 和 EMT scope 的 CPU 计划互不重叠。
OMS W7 k6 默认使用 `127.0.0.1:6565`；本工具默认使用不同的 loopback 管理地址
`127.0.0.1:6567`，必要时可通过 `--ems-k6-api-address` 显式改变，避免两个 k6 进程互抢
控制端口。该地址不承载 benchmark HTTP 流量。

## 本地热路径容量边界：`H1--H3`

`run_hot_capacity.py` 以阶梯 offered load 测量当前实现的本地、端到端 HTTP 热路径容量，而非
以空循环或 SDK stub 推算。它使用真实的 `ConsumerGatewayV2`、EMS control-plane、session-shard
router、child preparation、consumer checkpoint 与 immutable snapshot 实现，但**不会导入 EMT SDK、
不会创建 adapter、不会发送委托**。因此结果不能写成 EMT/场地下单 TPS。

```bash
PYTHONPATH=. .venv/bin/python -m tools.ems_strategy_load_platform.run_hot_capacity \
  --base-url http://127.0.0.1:18180,http://127.0.0.1:18181 \
  --k6-bin /path/to/k6 --fault-key "$FAULT_KEY" \
  --output-dir artifacts/performance/ems-strategy-hot-capacity/<run-id>
```

三个互不相加的边界为：

| workload | 分子和完成边界 | 单位 |
| --- | --- | --- |
| `H1-SIGNAL` | 策略信号通过 gateway 授权并被 EMS 确定性接纳 | TPS |
| `H2-EVENT` | deterministic contract event 被策略 consumer 完整 checkpoint/state apply | event TPS |
| `H3-QUERY` | 策略 immutable snapshot 的完整 HTTP 请求/响应 | QPS |

每个档位 reset 到固定状态、运行一个稳态 plateau；首个档位失败后，驱动会对前一档独立补足三轮。
只有三轮均零 dropped、零 HTTP failure、所有 workload latency contract 都通过，才可称为
`highest_three_run_stable_rate`。首个失败 offered rate 只定义失稳边界；若最高配置档仍通过，结果
只能写为下界，不能伪称已经测到硬饱和。

runner 同时校验 k6 的退出码和 summary export 内每一条 threshold。两者任一表示失败，都不能把该
档位视为 stable；这避免因外层进程状态、版本差异或摘要/退出码不一致而漏判 P95/P99 越线。

驱动默认在每个 workload 开始前预分配 `2,000/4,000/4,000` 个 k6 VU（H1/H2/H3），并把
`maxVUs` 固定为同一数值，避免 k6 启动期动态扩容本身产生 dropped iteration 后被误判为服务端
饱和。若提高最大 offered rate，必须按最坏调用延时相应提高这三个值，并在结果中记录 generator
VU 容量；若 generator 自己不足，整档只能标为无效，不得形成项目容量边界。

恢复某一条无效探索路径时可重复 `--workload H2-EVENT` 或 `--workload H3-QUERY`，避免已完成
的独立 H1 三轮被无意义重跑；每个新的结果目录仍必须完整保留 preflight、每档 k6/HDR 与结果 JSON。
