# TradeCore OMS 负载与故障演练平台

这是独立进程的 OMS sidecar 验收工具，覆盖已提交事实的 ingest、immutable snapshot query、watermark/health 和 HDR 指标。它不导入 EMT adapter、SDK、dispatcher 或 mmap writer，因此不会成为 `C0→C4A` 或 `C5→C7` 的同步依赖。

## HTTP 合同

| 接口 | 用途 | 成功语义 |
| --- | --- | --- |
| `POST /perf/v1/oms/facts` | 提交脱敏的 committed-fact 测试输入 | `201 PROJECTED`：scope actor 已按连续 position 应用该 fact；快照以 200 ms 合并窗口异步发布（低于默认 250 ms stale 门槛）。性能客户端可传 `X-TradeCore-Perf-Ack: minimal` 获得无 body 的同一 `201` 语义 |
| `GET /perf/v1/oms/orders/{execution_id}` | 单订单 immutable snapshot 查询 | `200` 或 `404`，不触发 EMT 查询 |
| `GET /perf/v1/oms/snapshot?require_fresh=true` | snapshot generation、watermark、authoritative/stale 状态 | stale 时 fail-closed 为 `503` |
| `GET /perf/v1/oms/health` | per-scope watermark、队列深度、暂停状态 | `READY` 或 `STALE` |
| `GET /perf/v1/metrics/hdr` | HDRHistogramV1 的 P50/P95/P99/P99.9/P100 | 只含计数与延时，不含账户、证券、订单号或凭据 |
| `GET /metrics` | Prometheus scrape 文本 | HDR 分位数 gauge；完整 HDR 仍以 JSON 接口为准 |

测试 payload 只能使用合成 ID。位置、observed time 和 payload hash 由服务生成，避免并发 HTTP 到达顺序被误当作原始 Journal 缺口。生产 Journal reader 仍使用 `OmsSidecarFactReaderV1` 的真实 position、epoch 与 route 校验。

## 启动与依赖

```powershell
.\.venv\Scripts\python.exe -m pip install -r tools\oms_load_platform\requirements.txt
.\.venv\Scripts\python.exe -m tools.oms_load_platform.server --host 127.0.0.1 --port 18080 --backlog 8192 --enable-faults --fault-key <temporary-key>
```

服务默认只绑定 loopback。`--backlog` 默认 `8192`，用于 W3/W7 k6 的高并发连接建立；它不改变 OMS actor queue 的有界容量，也不进入 EMT 热路径。故障接口仅在显式 `--enable-faults` 和一次性 key 同时存在时暴露；正常运行不应打开它。

## W0–W5：k6 自动门禁

| Gate | 场景 | 通过条件 |
| --- | --- | --- |
| W0 | 合同、fresh snapshot、health 与 HDR schema | 全部 HTTP/check 成功 |
| W1 | sustained fact ingest | `2,000 TPS`、无 4xx/5xx、HTTP P95 `<50 ms`、P99 `<100 ms` |
| W2 | immutable order snapshot | `5,000 QPS`、无 4xx/5xx、HTTP P95 `<25 ms`、P99 `<50 ms` |
| W3 | W1 + W2 同时运行 | 写/读都无失败，分别满足较宽的混合负载 tail gate |
| W4 | watermark/health 持续读取 | 所有 scope 为 READY、watermark 完整、无失败 |
| W5 | projector pause 后 fail-closed | 新 fact `503`、fresh snapshot `503`、health 为 STALE；恢复后才允许下个 gate |

W1--W4 还要求 k6 `dropped_iterations=0`；因此 2,000/5,000 是实际完整发送速率，不是只写在 scenario 配置中的目标值。

安装 Grafana k6 后运行：

```powershell
.\.venv\Scripts\python.exe tools\oms_load_platform\run_w0_w5.py --base-url http://127.0.0.1:18080 --fault-key <temporary-key> --output-dir artifacts\oms-load\w0-w5
```

每个 workload 保留 k6 原始 summary，并由 `w0-w5-gate-result.json` 以 [`RESULT_SCHEMA.json`](RESULT_SCHEMA.json) 汇总。W3 前，runner 会通过受 fault key 保护的 `reset-benchmark` 对每个 shard 关闭 admission、排空已受理事实、恢复固定 seed baseline，再采集各 shard 的 `HDRHistogramV1`；这让 W1 与 W3 是可复现实验，而不是前序合成数据无限增长后的偶然结果。k6 缺失时 runner 返回 `2`，不会伪造为通过。

单进程用于合同、replay 和低/中并发定位。`2,000 TPS + 5,000 QPS` 的全接纳门禁使用 **一个 scope 一个进程/CPU** 的分片形态：每个实例以 `--scopes 1` 运行于独立 loopback/内网端口，k6 的 `--base-url` 接受逗号分隔的全部 URL 并按 VU/iteration 稳定分流；runner 会把 `SCOPES=1` 传给 workload。各 shard 的 HDR 必须分别归档，不能将不含原始 buckets 的 percentile summary 伪合并成全局 HDR。

## JMeter：人工与分布式复刻

[`jmeter/oms-load-platform.jmx`](jmeter/oms-load-platform.jmx) 是 W1/W2 的原生 HTTP 复刻，适合 GUI 排错或在 Controller/Load Generator 已就绪时执行。它使用 JMeter 标准 `ConstantThroughputTimer`，单位是**样本/分钟**：

- W1 `write_samples_per_minute=120000`，即 2,000 TPS；
- W2 `read_samples_per_minute=300000`，即 5,000 QPS；
- 可用 `server_host`、`server_port`、`write_threads`、`read_threads`、`result_file` 覆盖默认值。

分布式模式必须从多个 Load Generator 汇总原始 `.jtl`，并在报告中标明各 generator 的时钟同步状态；不能把 JMeter GUI listener 当作压测端。

## W6：故障演练

W6 使用真实 HTTP 合同按顺序验证：replay digest 等价、projector pause、snapshot stale/fresh gate、paused ingest fail-closed、resume 后再 replay 等价。

```powershell
.\.venv\Scripts\python.exe tools\oms_load_platform\run_w6_fault_drill.py --base-url http://127.0.0.1:18080 --fault-key <temporary-key>
```

pause 只影响此独立服务的 actor。它不暂停 EMT session、native caller、callback writer 或任何真实交易进程。

## W7：与 EMT 499-burst 同时运行

W7 只能在同一次 EMT 权威 `QueryUnfinishedOrders` 证明未闭合订单为 `0` 后开始。2026-09-10 的当前 revision 已取得 W7 通过证据：W7 前后均为 query-only 零未完成订单，且没有在 query-only 步骤中发送撤单。此前的 `BLOCKED` 记录只保留为历史 fail-closed 证据，不能替代或覆盖每次新运行所需的 120 秒新鲜门禁。

解除后，W7 固定执行以下可比较的组合：

1. 同一 revision、同一 EMT scope/process、同一 CPU affinity 与相同 order-plan 的 EMT 499 baseline；
2. 在独立 CPU/进程运行 W3 的 `2,000 TPS + 5,000 QPS`，同时执行第二个同口径 EMT 499 burst；
3. 两轮都要求 `499/499` native accepted、`499/499 C7`、完整 callback 时间对、callback gap `0`，以及 query-after-cleanup `0`；
4. 输出 C0→C4A、C5→C7、C0→C7 的逐笔配对分位数和 499 batch wall-clock；对照轮与伴随轮均不得有完整性退化。只有先满足完整性，再评估相对 P95/P99 变化；不能用不同轮的独立分位数直接相减。

W7 不会把 OMS sidecar 写入 EMT Journal，也不会把 k6 流量变成 EMT 下单。它证明的是旁路负载与真实 EMT hot path 并行运行时的非干扰性。

[`run_w7_parallel.py`](run_w7_parallel.py) 已固化这一门禁：它只接受 120 秒内生成、`QueryUnfinishedOrders=0` 且未发起撤单的 query-only recovery proof；先对每个 OMS shard 建立干净基线，确认 W3 已在每个 shard 实际受理 fact，才并发启动 W3 和一个经审核的 JSON `argv` EMT 499-burst 命令。它还要求 EMT result 明确证明 `499/499` native accepted、C7、完整 C5→C7 时间对、零 callback gap 和完整撤单终态；进程退出码本身不能作为 W7 通过证据。不满足任一条件都会以 `W7 BLOCKED` 或 `W7 FAIL` 退出，且不得将部分结果标为通过。

W7 在启动任何 k6 或 EMT 子进程前，还会验证 EMT JSON `argv` 中唯一的 `--output` 与 `--emt-report` 完全一致，避免出现已发起模拟 burst 却无法读取对应证据的错误配置。若 EMT scope 已做 CPU affinity，应使用 `--k6-cpu-list` 将 k6 固定到与 EMT scope、OMS sidecar 均不重叠的 CPU，防止负载发生器干扰热路径测量。

## LoadRunner

本平台已固定 HTTP contracts、HDR 输出与结果 schema；LoadRunner 只在许可证、Controller 和 Load Generator 都已提供后接入相同 contracts。当前不包含或假定任何 LoadRunner 许可证。
