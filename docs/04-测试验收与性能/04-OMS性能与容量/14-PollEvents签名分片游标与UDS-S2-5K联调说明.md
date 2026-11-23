# PollEvents 签名分片游标与 UDS-S2-5K 联调说明

日期：2026-09-30。服务端功能提交：`3d5b6f7`，跨账户验收提交：`fdcd912`。

## 1. 已提供的能力

采用“由服务端为每个区间签发独立 signed cursor”的方式。新增正式 UDS operation：
`CreateReplayPartitions`。压测者可自行调用，不需要 TradeCore 运维逐个生成 token。

当前交付目标为 Linux 服务器上既有的无 Adapter Consumer UDS：

- 服务：`xuanling-tradecore-consumer-uds.service`。
- socket：`/run/tradecore-consumer-uds/consumer-v2.sock`。
- 传输：同主机 `AF_UNIX/SOCK_STREAM`、XTC2 framing、原有 UID/GID principal 授权。
- 后续分页：继续调用原有 `PollEvents(cursor, limit)`，不增加客户端可篡改的 LSN 参数。
- 不调用 EMT/QMT，也不把合成 OMS 事件标成柜台回报。

协议为现有 V2 的增量 operation，旧 operation/frame/golden vector 不变。
request schema 与合同摘要已更新；压测报告应采集当前实际合同与安装包摘要，不沿用旧值。
当前 schema_hash 为 `9289c84edc783e4fe9b20cbb7bc5396423c40d4491095188d6b5dfa7c6b234e7`。

## 2. 一次创建分片

每轮先完成受控 reset、服务就绪检查、测试数据预置，再调用一次：

```json
{
  "message_type": "request",
  "schema_version": 2,
  "operation": "CreateReplayPartitions",
  "request_id": "s2-r1-partition-plan",
  "payload": {
    "consumer_account_id": "server-local-uds-benchmark",
    "partition_count": 32
  }
}
```

`consumer_account_id` 必须取本轮 `ListAccounts` 返回的授权账户；以上为既有 benchmark
binding 的示例值。`partition_count` 为整数 `1..64`，不能传布尔值或额外路由字段。
实际分片数为 `min(partition_count, 可重放事件数)`；没有数据时返回 `partitions=[]`、
`event_count=0`、`high_watermark_position=null`，不签发会跟随后续数据增长的空区间。

响应 `status=COMPLETED`，payload 字段如下：

| 字段 | 类型与含义 |
| --- | --- |
| consumer_account_id | 本次授权的消费账户 |
| journal_id | 本轮 Journal 身份，reset 后改变 |
| stream_generation | 流代次，必须与 Journal 身份一起记录 |
| event_count | 此次固定快照内真实可消费事件总数 |
| high_watermark_position | 整个快照的最后位置 |
| partitions | 相邻、不重叠的分片数组 |
| partitions[].partition_id | 从 0 开始的本 plan 内编号，不是跨轮次租约 ID |
| partitions[].after_position | 本分片起点，不包含该位置；第一个分片为 null |
| partitions[].high_watermark_position | 本分片终点，包含该位置 |
| partitions[].event_count | 本分片期望事件数，所有分片之和等于顶层 event_count |
| partitions[].cursor | 服务端签发的初始游标，原样交给 PollEvents |

区间为 `(after_position, high_watermark_position]`。相邻分片的前一终点等于后一
after_position。按真实事件位置分配，不假定 LSN 连续；不能用 LSN 差值代替事件数。

当前 target 的 cursor TTL 为一小时。签名绑定 consumer/account、Journal、generation、
authorization epoch、有效期、当前 after 与固定终点；后续 next_cursor 保留原分片终点。
在 plan 创建后新增的事件不进入该 plan。

## 3. 每个 worker 的调用方式

每个 worker 独占一个分片；不要让多个 worker 同时使用相同 cursor。相同 token 的重复
读取是合法的可重放行为，服务端不提供自动领取/租约/消费组抢占，也不承诺 exactly-once delivery。

```python
from pathlib import Path
from xuanling_tradecore.consumer import ConsumerUnixClientConfigV2, ConsumerUnixClientV2

# plan 创建在测量窗口外；大数据集首次构建索引可使用较长准备超时。
setup_client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(
    Path("/run/tradecore-consumer-uds/consumer-v2.sock"), timeout_seconds=120.0
))
response = setup_client.call({
    "message_type": "request", "schema_version": 2,
    "operation": "CreateReplayPartitions", "request_id": "s2-r1-partition-plan",
    "payload": {"consumer_account_id": "server-local-uds-benchmark", "partition_count": 32},
})
if response.get("status") != "COMPLETED":
    raise RuntimeError(response.get("code", "partition plan failed"))
plan = response["payload"]

# 将 plan["partitions"] 中的每一项仅分配给一个 worker。
partition = plan["partitions"][0]
client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(
    Path("/run/tradecore-consumer-uds/consumer-v2.sock"), timeout_seconds=5.0
))
page = client.call({
    "message_type": "request", "schema_version": 2,
    "operation": "PollEvents", "request_id": "s2-r1-p0-page0",
    "payload": {"cursor": partition["cursor"], "limit": 100},
})
```

上例展示真实调用方式，未代替测试者实现事件处理与 checkpoint 存储。每页必须按顺序：

1. 检查 response/status、分片 high watermark、事件位置递增与事件去重。
2. 处理事件，在客户端自己的持久化存储中将处理结果与 `next_cursor` 原子提交。
3. 提交成功后，把该页新增唯一事件数计入 checkpointed event TPS，再用 next_cursor 请求下一页。
4. `has_more=false` 时该分片已完成，停止该 worker；禁止把初始 cursor 重新循环来制造吞吐。

checkpoint 至少记录本轮 plan request ID、journal_id、generation、partition_id、上下界、
最后处理位置和 next_cursor。游标不透明；客户端不修改 token，不获取服务端签名密钥。
报告保存 token 摘要即可。

分片内必须有序；各 worker 的响应到达顺序不是全局事件顺序。需要全局有序视图时按完整
position 合并；可使用官方 `ConsumerEventPositionV2` 解析/比较位置，不按位置字符串排序。

## 4. UDS-S2-5K 的计数与准备

目标是 **5,000 个 checkpointed 唯一事件/秒 × 30 秒**，不是 5,000 次 PollEvents RPC/秒。
例如每页 100 个事件，理想条件下约 50 个成功分页/秒即可提供 5,000 event/s。
报告应分别列出 RPC QPS、received event/s、processed event/s 和 checkpointed event/s。

- 每轮准备不少于 150,000 个测量事件，另加预热与余量；建议预置至少 200,000 个唯一
  synthetic OMS accepted events，先确认 Journal 容量。Journal fact 数和事件数不是同一计数。
- 当前部署容量为 524,288 个 fact 槽，文件预分配约 2 GiB，每笔新接纳写入两个 fact，
  仅含新接纳时可容纳 262,144 笔；独立 UDS unit 的 MemoryMax 为 4 GiB。
  旧容量 262,144 个 fact 槽最多容纳 131,072 笔，不能完成本 profile 的 150,000 事件数据准备。
  混合写入、幂等冲突等也会消耗 Journal 容量，需记录实际余量。
- 数据预置、分片签发、索引建立均在测量窗口外。一次 plan 供整轮使用；worker 从预热后已
  持久化的 checkpoint 进入正式窗口，不能在测量中反复签发 BEGIN 分片。
- 32 个分片是初始建议，不强制创建 4096 个并发轮询 worker；可以固定 8/16/32 进行比较，
  每轮在 profile 记录 partition_count、limit、worker 数及连接模式。
- 三轮各自 reset、预置、创建新 plan。reset 会更换 Journal 与签名密钥，旧 cursor 全部作废。
- 跨分片核对唯一 event_id、各分片实收数与 event_count；有明确原因的重试必须去重，不重复计 TPS。
- dataset 提前耗尽就报告 DATASET_EXHAUSTED，不能空轮询或重复读取来凑 150,000。
- 原标准的 P95≤10 ms、P99≤40 ms、无 gap/未解释重复与观测覆盖要求保持。
  游标能力功能通过不代表整体压测 PASS。

错误处理：篡改/错绑定游标为 CURSOR_INVALID，过期为 CURSOR_EXPIRED；未授权账户签发为
UNAUTHORIZED；未装配分片 reader 的其他 Gateway composition 返回 CURSOR_RECOVERY_UNAVAILABLE。
旧服务不认识新 operation，须核对服务端实际部署与合同摘要后再测试。

## 5. 已完成的验证

### 客户端合同校验与证据导出

TradeCore 提供窗口外使用的官方辅助函数，避免各压测实现自行猜测字段名：

```python
from xuanling_tradecore.consumer.replay_evidence_v2 import build_replay_partition_evidence_v2

# response 为本轮 CreateReplayPartitions 的真实响应，不是 PollEvents 响应。
if response.get("status") != "COMPLETED":
    raise RuntimeError("partition creation failed")
plan_evidence = build_replay_partition_evidence_v2(response["payload"])
# 将 plan_evidence 与 plan request ID 摘要一起保存到本轮证据目录。
# 私有 worker/checkpoint 存储仍使用原始响应的 cursor；导出文件不含完整 token。
```

辅助函数验证正式字段存在、合法 BEGIN、数值位置递增、相邻分片衔接、事件数汇总、最终
watermark、Journal/generation，并导出原始字段名及 cursor SHA-256。字段缺失与字段值为
null 不等价；不得用 start_position/end_position 替代正式字段。
它不验证服务端签名、不执行 checkpoint、不宣称 S2 性能通过，且不在服务端热路径运行。

运维只读合同探针（不预置、不消费事件、不重启服务、不创建持久 checkpoint）：

```bash
cd /opt/xuanling-tradecore
.venv/bin/python tools/performance/consumer_uds_replay_probe.py \
  --config /etc/xuanling-tradecore/consumer-uds.json \
  --partitions 2 --expected-events 200000 --contract-only \
  --output /tmp/s2-plan-contract-UNIQUE-RUN-ID.json
```

expected-events 必须填写当前数据集真实数量；空服务填 0，可验证空计划合同但不证明非空
分片正确。输出文件必须不存在。每次读取都会生成新的 plan，此探针不能冒充历史压测
当时的分片响应。普通 drain 探针同样附带 plan_evidence；只作功能检查，不作正式 S2 验收。

### S2 结果判定边界

- `RPC latency`：请求调用开始至响应校验完成；每页一个样本。
- `post-response checkpoint latency`：响应完成至客户端持久化提交成功。
- `request-to-checkpoint latency`：请求开始至持久化提交成功，必须逐页计算。
- 统一标准消费端完整延迟仍为 `G7/S0 → consumer receive → checkpoint applied`；
  合成重放 profile 必须标明 S0 的释放定义和时间点，不把历史 seed 写入时间冒充本轮释放，
  也不能只用 RPC 分位数代替完整消费延迟。P95≤10 ms、P99≤40 ms 不变。
- checkpointed event/s 按实际提交完成时间计窗口内唯一事件。30 秒平均 5,000 event/s
  不是每个 1 秒桶都至少 5,000；逐秒速率、队列/lag 和尾延迟应独立保留。
- 吞吐、RPC、checkpoint 持久化、边界证据和完整消费延迟分别判定。任何一项未满足，
  不得通过一个总 PASS 隐去；吞吐通过不等于完整标准通过。
- 若只丢失报告字段，可依据当时保存的真实响应离线补正；没有原始响应则报告缺证，
  不把新探针回填为旧证据。修正 checkpoint 实现后的性能验收需独立 reset 后重测三轮。

本地全量测试 705 passed、32 skipped；schema 快路径等价检查和修改文件类型检查通过。Linux 9 项专项通过，
包括：空数据集、非整除分片、所有区间并集完整、next_cursor 终点保持、后续新增事件不越界、
篡改签名拒绝、跨账号拒绝、epoch/有效期检查，以及原有持久化与 UDS 回归。

已部署服务真实 UDS 功能探针：2,000 个唯一 SubmitSignal 全部 accepted，签发 32 个分片，
每页 limit=17，32 个 worker 并行读取 128 页，共 2,000 个事件且全部唯一，零重复/遗漏，
所有终点和完成后的空页检查通过。功能探针约 51.21 ms；没有实现持久化 checkpoint，
因此该耗时不得换算为本标准的 checkpointed TPS。

服务器功能证据：`/tmp/tradecore-replay-partitions-ipozr0p3/summary.json`。
客户端现在可按本文自行签发，不需要运维交付一批短期有效的手工 token。

## 6. 服务端优化与逐请求 trace 交付

PollEvents 使用绑定当前 schema 摘要的正向快速校验；不匹配的输入仍使用完整 JSON Schema。
签名、账户绑定、epoch、有效期、Journal generation、页内容校验保持不变。页内顺序与去重
改为等价的线性严格递增检查，不重复排序和构造集合；上界检查在严格递增成立后检查尾元素。
只读投影缓存复用不可变分页对象；reactor 每轮最多处理 64 个请求，并在达到 1 ms 连续 dispatch 时间预算后
让出到 socket 事件处理。该预算不是抢占式执行，不能中断一个长 handler。

独立 no-Adapter UDS systemd unit 设置 `TRADECORE_UDS_TRACE=1`；其他运行组合默认关闭。
每次 reset 自动生成当前 Journal 对应的文件：

```text
/var/lib/tradecore-consumer-uds/traces/<journal_id>.jsonl
/var/lib/tradecore-consumer-uds/traces/<journal_id>.status.json
```

文件仅服务用户可读写，旧轮次保留。使用当前 `CreateReplayPartitions` 响应中的 journal_id
定位，不以目录中最新文件猜测轮次。已有同用户联调进程可读取，不需要扩大 socket/目录权限。
trace 队列上限 65,536 条，单 namespace 文件上限 512 MiB，异步批量写入，无交易线程 fsync。
status 包含 submitted/written、queue_dropped、capacity_dropped、writer_failed、queue_depth
和 peak_queue。采集前等待 writer 排空、status 更新；任何丢弃或失败均不能宣称完整覆盖。
达到文件上限后不会自动删除旧证据，应结束该轮并受控 reset，不可忽略 capacity_dropped。

逐请求字段不含账号、订单内容、密码、cursor 或原始 request_id。关联键为
`SHA256(request_id.encode("utf-8"))` 的完整 64 位小写十六进制；客户端必须记录同一摘要。
异常 Unicode surrogate 使用 surrogatepass，仅正常 UTF-8 request_id 用于标准压测。
还记录 correlation_id、operation、响应种类、实际发送完成状态、请求/响应字节数和队列深度。

| 时间点 | 精确定义 |
| --- | --- |
| first_byte_observed_ns | 服务端 recv 观察到首批字节；不代表内核到包时间 |
| frame_ready_ns | 已有完整长度 frame，进入待处理队列；尚未完成 CRC/schema 验证 |
| dispatch_started_ns | 开始处理请求，包含首次 session 建立 |
| frame_decoded_ns | decode/CRC 完成 |
| schema_validated_ns | 请求 schema 校验完成，不冒充全部业务授权完成 |
| cursor_authorized_ns | PollEvents cursor 签名/绑定/epoch/有效期/generation 验证完成 |
| replay_read_finished_ns | 读取不可变回放页完成 |
| replay_validated_ns | 回放页身份与边界检查完成 |
| next_cursor_signed_ns | 非空页的 next_cursor 签发完成；空页没有此阶段 |
| event_payload_built_ns | 事件响应字典和 position 编码完成 |
| handler_finished_ns | handler 返回；不是客户端持久化 checkpoint |
| response_encoded_ns | XTC2 响应编码完成 |
| send_finished_ns | 非阻塞 send 累计完成全部响应字节 |

时间统一使用服务器 `time.monotonic_ns`，status 附 wall/monotonic 锚点。统计各时间对必须逐请求
计算，不能相减总体分位数。FRESH 模式每个连接一个请求；若将来采用 pipelined persistent
模式，同次 recv 已读入后续 frame 的 first_byte 可能共用观察时间，必须另列 profile。
pre-admission 拒绝、半帧与无 request_id 的流量还要结合 ingress counters；这个请求 trace
不替代客户端 U0/U6/U7 打点或真实 OMS commit 点，不宣称单靠它覆盖全部 UDS 总体验收。

服务端只读汇总示例（时间窗使用本轮服务器 CLOCK_MONOTONIC 数值，单位 ns）：

```bash
cd /opt/xuanling-tradecore
.venv/bin/python tools/performance/consumer_uds_trace_report.py \
  --trace /var/lib/tradecore-consumer-uds/traces/<journal_id>.jsonl \
  --operation PollEvents --start-ns <start_ns> --end-ns <end_ns> \
  --expected-count <本轮窗口内RPC数> --output /tmp/<run_id>-server-trace-summary.json
```

工具的 request_count_coverage_pass 只校验数量、正常响应和 writer 无丢失，仍须与客户端
request_id_sha256 做一对一关联。原始 jsonl 与 status 一起归档；禁止把返回 cursor 的 SHA-256
链称为客户端持久化 checkpoint。客户端须保存可用于恢复的完整 token（私有存储）和处理结果，
证据只公开其摘要、持久化事务标识、提交时间和最后位置。

正式 S2 每轮需要连续运行完整 30 秒，目标 5,000 个已持久化 checkpoint 的唯一事件/s。
可用 limit=100、约 50 个分页请求/s 的聚合调度保持目标速率，worker 初始选 2–4，先预置
200,000 条再预热；分页额度按实际事件数计，提前耗尽标记 DATASET_EXHAUSTED。另立无节流
drain profile 测短时最大读取能力，不把 1.4 秒读完的事件数除以 30 后宣称稳态通过。
重放输出和 checkpoint 提交时间分开打点。P95≤10 ms、P99≤40 ms、三轮、无 gap/未解释重复
的门槛不变。每页 RPC 延时计一个样本；按事件重复该页延时必须明确标 event-weighted，
不能冒充逐事件处理或 checkpoint 延时。按实际 API 保存 after_position/high_watermark_position，
不得读取不存在的 start_position/end_position 后丢弃分片边界。

## 7. 本次部署验证记录

2026-09-30，运行代码 `ca73537`：全量本地测试 919 passed、34 skipped、273 subtests；
修改的运行模块 Ruff/Pyright 通过。Linux 专项覆盖真实 SO_PEERCRED/UDS、签名分片、
慢读与半帧、CRC、跨账户/epoch/过期拒绝、有界背压、trace 队列/文件满与线性校验等价性。
服务器按 dev 分支 Git 拉取部署，不涉及 EMT/QMT，不重启整机。

真实部署探针先接纳 20,000 个唯一 synthetic signals，再对同一固定数据集分别执行
2/8/32 分片诊断；每组完整读取 20,000 个唯一事件，无重复/遗漏。三个诊断组不能合并计为
60,000 个全局唯一事件。limit=100、fresh-per-request、trace 开启，数据准备与签发不计入表中 RPC 延时：

| worker/分片 | RPC数 | P50 ms | P95 ms | P99 ms | P100 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2 | 200 | 0.827555 | 1.772066 | 1.862322 | 2.071958 |
| 8 | 200 | 4.220911 | 8.053426 | 9.993796 | 12.141888 |
| 32 | 224 | 17.292597 | 41.007007 | 52.022891 | 85.561442 |

624 个 PollEvents 请求均用 request_id_sha256 与服务器 trace 一对一匹配，时间对无负值；
整份 trace 写入 20,627 条，submitted=written，queue_dropped=capacity_dropped=0，
writer_failed=false，peak_queue=11。2-worker server_handling P95 为 0.672081 ms；
32-worker ready_queue P95 为 12.951526 ms，server_handling P95 为 13.409325 ms。
不同链路分位数不得相减归因，也不能由此把客户端 85.56 ms 长尾全部归因到服务端。

该探针没有客户端 durable checkpoint，且每组不是连续 30 秒负载，**不作为正式 S2 PASS**。
它证明当前部署、分页边界、完整性及 trace 关联可用，32-worker 无节流仍有尾延时，不承诺
任何并发设置自动达到 SLA。测试者按第 6 节修正正式 profile 后独立完成三轮。

服务器证据目录：`/tmp/tradecore-s2-final-ss9uqxz1/`。
持久 trace：`/var/lib/tradecore-consumer-uds/traces/consumer-uds-91b6674719eb49388674611e76fd38b2.jsonl`。
本地副本：`artifacts/performance/uds-s2-server-optimization-20260930/`（不进入 Git）。
部署探针结束后受控 reset，旧 Journal/trace 保留，正式测试必须使用自己本轮新建的 plan。
