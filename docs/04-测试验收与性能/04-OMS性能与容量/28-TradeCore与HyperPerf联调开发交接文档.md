# TradeCore 与 HyperPerf 联调开发交接文档

交接日期：2026-11-06。对象：负责将 HyperPerf 与 TradeCore 接通的开发、测试及运维人员。

本次交付文档和只读核对结果，不代表恢复先前暂停的 Runner、桥接或压测开发，也不授权
部署、重启服务或发压。负责人确认 HyperPerf 下一阶段能力及执行范围后，再推进实施。

## 1 联调目标与结论

联调最终需要同时证明：消费者确实调用 TradeCore、业务完成边界正确、HyperPerf 记录真实
吞吐与延时，以及资源监控绑定到了承受负载的 TradeCore 进程，而不是发生器或示例靶。

三类目标必须分开登记，不能用一个结果替代另一个：

| 目标 | 真实链路 | 结论边界 |
| --- | --- | --- |
| 平台脚本联通 | Locust → HyperPerf 受控代理 → `/work` 隔离靶 | 验证上传、执行、代理、断言、统计和产物，不进入 TradeCore |
| TradeCore HTTP 组件 | Locust → 受控代理 → 独立 OMS 或 EMS/策略 HTTP 工具 | 进入部分真实内部组件；不经过 Consumer UDS，不调用 EMT，不证明生产 Journal 持久化 |
| TradeCore Consumer | 独立消费者 → 真实 UDS → Consumer/OMS/Journal → 响应或事件回放 | 验证常驻 Consumer 本地链路；使用 no-Adapter 配置，不向柜台下单 |

现有 Locust Runner 基线只接受 HTTP/WebSocket，不能仅换 host 就连接 UDS。原生 UDS
需要 HyperPerf 提供正式、受控的接入能力；HTTP 到 UDS 测试桥只是候选，尚未实施或选定。
如果未来采用桥接，HTTP 端到端与 UDS/服务端耗时必须分开，不能把前者写成原生 UDS 性能。

本轮不包含 EMT/QMT 委托、Adapter 凭据配置、撤单、行情、故障演练、恢复放行或 READY。

## 2 版本与代码入口

### 2.1 TradeCore

| 项目 | 信息 |
| --- | --- |
| 产品 | TradeCore；旧仓库名保留为兼容路径 |
| Git 地址 | `ssh://git@192.168.251.1:2222/liu/emt_gateway.git` |
| 对接分支 | `dev`；先拉取再固定实际 commit，不按分支名代替版本证据 |
| 本文基于的本地提交 | `e8242e88ceae7d0768339a448846550dd8dd0f6e` |
| HTTP 脚本交付提交 | `d3e587cd484a70e524fb246b6c1010494a539a15` |
| 服务器正式目录 | `/opt/xuanling-tradecore` |
| 2026-11-06 该目录 Git HEAD | `967149c935c4d36940ca9b88e6760c58fb844bea` |
| 实际解释器 | `/opt/xuanling-tradecore/.venv/bin/python`，本次为 CPython 3.10.21 |
| 实际导入位置 | `/opt/xuanling-tradecore/.venv/lib/python3.10/site-packages/xuanling_tradecore/` |

**源码 checkout、已安装 wheel、运行进程是三个独立版本对象。** 上述服务器 Git HEAD 不证明
运行 wheel 就是该提交；不得因 `git pull` 成功便宣称服务已更新。本文没有重装 wheel 或重启。

对接方可在自己的授权 checkout 拉取，但不要擅自在正式目录部署：

```bash
git remote get-url origin
git status --short
git pull --ff-only origin dev
git rev-parse HEAD
```

工作区不干净或 Git 认证失败时，保留现场并联系负责人，不 reset、不改 remote、不借用
其他项目的私钥。Git SSH 用户为 `git`、端口为 `2222`，与 Linux 登录用户/端口不同。

### 2.2 HyperPerf

对接方此前提供的部署基线为 `3e442a9`，Locust 扩展为 `io.hyperperf.locust@1.0.0`，
使用 `locust==2.31.5`。下一阶段是否已更新，由 HyperPerf 开发核对并提供锁定版本。

参考源码工作副本位于原维护者本机
`D:\xuanling\总项目\基建类\压测系统\HyperPerf-agent-mock-dev`；
这不是另一位开发必须复用的工作目录，也不能代替服务器部署证据。

## 3 服务器与登录权限

### 3.1 主机信息

| 项目 | 值 |
| --- | --- |
| Linux 主机 | `192.168.251.1`，主机名 `xuanling` |
| SSH | TCP `22` |
| 操作系统基线 | Ubuntu 24.04.4 LTS，Linux x86_64 |
| CPU | AMD Ryzen Threadripper PRO 9995WX，96 核 / 192 线程 |
| 系统可见内存 | 约 377 GiB |
| GitLab SSH / Web | `2222` / `9080` |

HyperPerf 与 TradeCore 在同一服务器，不等于沙箱中的 `127.0.0.1` 或 `/run` 与宿主机相同。
HTTP 地址必须由平台受控网络代理解析；UDS 路径必须经正式隔离/授权机制暴露。

SSH 连接格式：

```bash
ssh -p 22 YOUR_ASSIGNED_LOGIN@192.168.251.1
```

`YOUR_ASSIGNED_LOGIN` 由管理员分配，不是 literal 用户名。密码、私钥或 GitLab token
通过团队受控渠道单独发放，不写入本文、脚本包、计划、请求体或报告。

### 3.2 账号与实际 UDS 身份

2026-11-06 只读核对结果：

| 账号 | UID / 主 GID | 与本目标有关的状态 |
| --- | --- | --- |
| `xuanling` | `1000 / 1000` | 运维账号，属于 sudo 组；no-Adapter UDS 已配置该身份 |
| `yangxue` | `1002 / 1003` | 测试账号，属于 `tradecore-testers`；UDS additional peer 已配置该身份 |
| `fengyu` | `1001 / 1001` | 属于 `tradecore-testers`，但本次读取的 no-Adapter peer 配置未包含该身份 |

文件访问权限与应用身份授权是两道门。`fengyu` 即使能打开 socket，也不等于通过
SO_PEERCRED 授权；本文没有尝试其 UDS 调用，不能预先写成已通过或保证具体失败形式。

HyperPerf 的 Runner 实际 host UID/GID 尚待对接方提供。容器用户名、容器 root、补充组
成员关系都不能代替服务端看到的 host peer UID/GID。禁止用 `chmod 777`、禁用 peer 校验、
改成管理员身份或绕过沙箱解决接入。需要新增身份时，由 TradeCore 运维按最小权限评审配置。

## 4 目标清单与实时核对

以下状态核对于 2026-11-06，不是未来执行时的保证；每轮需重新预检。

| 目标 | 地址或路径 | 本次核对结果 |
| --- | --- | --- |
| HyperPerf 示例靶 | `http://127.0.0.1:18981` | 端口监听；既有 `/work` 合同为 200 和 accepted=true；本次没有请求 `/work` |
| OMS HTTP 工具默认端口 | `127.0.0.1:18080` | 未发现监听；不是已部署可测的 origin |
| EMS HTTP 工具默认端口 | `127.0.0.1:18180` | 有监听，但只读 `GET /perf/v1/ems/health` 返回 **404**；不能认定为所需 EMS 服务 |
| Consumer no-Adapter | `/run/tradecore-consumer-uds/consumer-v2.sock` | listener 存在，服务 active/running；一次 ListAccounts 返回 COMPLETED |
| EMT 模拟盘 Consumer | `/run/tradecore-emt-simulator/consumer-v2.sock` | 对应服务 inactive/dead，不作为本轮目标 |

18180 的占用方未在本次确认。不要终止该进程、复用端口或将 404 写成业务成功。实际 HTTP
工具的 origin、端口、服务身份和白名单由两边负责人另行确认；必要时选择新的空闲端口。

### 4.1 Consumer 的实际配置

| 项目 | 当前值 |
| --- | --- |
| systemd unit | `xuanling-tradecore-consumer-uds.service` |
| 当前 PID | `480477`，仅是本次快照，不写死为监控 selector |
| 配置 | `/etc/xuanling-tradecore/consumer-uds.json` |
| Journal 根目录 | `/var/lib/tradecore-consumer-uds` |
| Consumer 账户别名 | `server-local-uds-benchmark` |
| 策略 ID | `uds-benchmark` |
| venue / adapter | `NO_ADAPTER_BENCHMARK` / `disabled-no-dispatch` |
| ListAccounts 状态 | `DISARMED`、reconciliation `NOT_REQUIRED` |
| socket 父目录 | `0750 xuanling:tradecore-testers` |
| socket | `0660 xuanling:tradecore-testers` |
| 配置 listen_backlog | `8192`；本次 ss 显示有效 listener Send-Q 为 `4096` |
| preframe / pending | `8192` / `2048`；不是全部请求零排队的承诺 |
| max_requests_per_connection | `1`，本 profile 使用每请求新建连接 |
| journal_capacity | `1,000,000`；不等于可写一百万个新订单，事实数与订单数需分开预算 |

`DISARMED` 在这里是 no-Adapter 压测配置的预期边界，不应为联通测试改成 READY。
旧标准中的 `0700/0600` 部署示例与当前受控测试组 `0750/0660` 存在差异，应在本轮
profile 和权限审查中明确，不能忽略差异后直接宣称旧门禁全通过。

### 4.2 资源配置

Consumer unit 当前 AllowedCPUs 为 `64-79,160-175`，MemoryMax 为 `6 GiB`，
LimitNOFILE 为 `65536`，TasksMax 为 `256`。cgroup 路径为：

```text
/sys/fs/cgroup/tradecore.slice/xuanling-tradecore-consumer-uds.service
```

AllowedCPUs 只是允许集合；独占性和 effective cpuset 需在执行前核对。发生器、桥接
（如采用）及采集器不能占用 Consumer 的专用 CPU。MemoryMax 不是已经预留给该进程的
物理内存，RSS 也不是整个服务及其 Journal 文件缓存的总量。

## 5 现有 HTTP 联通脚本

入口位于仓库 Python 根目录的：

```text
tools/hyperperf/http_smoke/hyperperf_http_smoke.py
tools/hyperperf/http_smoke/operation_names.json
tools/hyperperf/http_smoke/README.md
tests/test_hyperperf_http_smoke.py
```

该脚本是自包含 `HttpUser`，每 task 一次 `self.client.get('/work', ...)`，操作名固定为
`GET /work`；host 和调度由 HyperPerf 提供。严格检查 200、JSON Content-Type 和
`{"accepted": true}`，拒绝重定向，连接/读取超时为 2/5 秒，不自动重试。

交付提交的 44 项测试使用 Locust API 替身，证明离线断言逻辑，不证明服务器已加载 Locust
或平台网络已联通。本机脚本和 README 有后续未提交调整，不属于上述远端交付提交；
本次不合并这些改动。对接方使用所选 commit 的文件和 SHA-256，不依赖维护者工作区状态。

### 5.1 既有 HyperPerf 环境

| 配置 | 此前对接方提供的值 |
| --- | --- |
| 环境名称 | `FE-UAT-20261013-01-env` |
| 环境 ID / 版本 | `79bb0692-c75e-4807-947f-b27504857d59` / `8` |
| 场景类型 | `HTTP` |
| host | `http://127.0.0.1:18981`，不能附带路径 |
| operation_names | `["GET /work"]` |
| 白名单 | `127.0.0.1:18981` |
| 依赖 | `locust==2.31.5`，无额外脚本依赖 |
| 认证 / CA | HTTP 示例靶不需要；不发送 Proxy-Authorization |

环境是否仍是此版本，由 HyperPerf 管理员在计划预检时确认。这个白名单不自动允许
TradeCore 的其他端口。新目标要创建或更新受控环境版本并重新预检，不能自行改冻结快照。

## 6 独立 HTTP 组件合同

该路线用于 OMS、EMS、策略内部组件测试，不是常驻 Consumer 进程的替代品。对应服务
当前 origin 未确认；代码中的 18080/18180 只是默认值。

### 6.1 OMS 接口

实现为 `tools/oms_load_platform/server.py` 和 `service_v1.py`。

| 操作及固定 name | 请求 | 成功判断 |
| --- | --- | --- |
| `POST /perf/v1/oms/facts` | JSON fact，见下例 | 201、status=PROJECTED、scope 匹配、position 为有效正整数 |
| `GET /perf/v1/oms/orders/{execution_id}` | URL 中的 execution_id 取真实 seed 或本轮已写入数据 | 200，返回 execution_id 与预期一致，状态/版本满足 fixture |
| `GET /perf/v1/oms/snapshot` | 可选 `require_fresh=true` | 200；需 fresh 时 stale=false，并核对 generation、水位及数据基数 |
| `GET /perf/v1/oms/health` | 无 body | 200、READY、scope actor 状态及水位有效 |
| `GET /perf/v1/metrics/hdr` | 窗口外核对 | HDRHistogramV1 结构，计数对应本轮 |

`{execution_id}` 是稳定统计模板，不用实际 ID 作为 operation_name。

测试 fact 示例，仅向隔离工具提交：

```json
{
  "scope_id": "bench-scope-0",
  "execution_id": "hp-demo-execution-000001",
  "canonical_client_order_ref": "hp-demo-client-ref-000001",
  "kind": "PRE_NATIVE_COMMITTED",
  "order_state": "PRE_NATIVE_COMMITTED",
  "state_version": 1
}
```

`scope_id` 必须是实际创建的 scope。默认 seed ID 形如 `seed-000000`，数量由启动参数
`--seed-orders` 决定；不能拿任意伪造 ID 测查询成功率。并发写入的 ID 使用本轮唯一前缀。

201 表示 fact 已应用到 actor，合并快照异步发布，不能立刻把短暂查不到新 ID 判为数据丢失。
可选 `X-TradeCore-Perf-Ack: minimal` 返回无 body 的 201，但联通阶段使用完整响应，不混用
两种响应体合同。本工具不代表 mmap Journal 物理持久化或 EMT 执行。

### 6.2 EMS 与策略接口

实现为 `tools/ems_strategy_load_platform/server.py` 和 `service_v1.py`。

| 操作 | 请求及完成边界 |
| --- | --- |
| `POST /perf/v1/ems/plans` | request_id、wait_for_child；false 时 202 接纳，true 时 201 且 READY、child_count>0、shard_ids 非空 |
| `POST /perf/v1/strategy/signals` | 同上；经过 Consumer 授权测试 seam 再入 EMS，不是真实下单 |
| `GET /perf/v1/ems/plans/{plan_id}` | 真实 plan_id；200 且 ID、状态、子指令信息符合所选阶段 |
| `GET /perf/v1/ems/snapshot` | 200，generation/watermark/plan_count/ready_plan_count 有效，正常 profile 要求 stale=false |
| `GET /perf/v1/strategy/snapshot` | 200，generation/watermark/checkpoint_count 对应数据，正常 profile 要求 stale=false |
| `POST /perf/v1/strategy/events` | 独立 consumer_id、scope_id、连续 sequence、event_id；201、CHECKPOINTED、checkpoint 等于提交序号 |
| `GET /perf/v1/ems/health` | 200、READY、ems_admission=OPEN、strategy_consumer=OPEN |
| `GET /perf/v1/strategy/health` | 与 EMS health 同一实现 |

两种计划请求体共用路径，但必须分开 name，例如：

```text
POST /perf/v1/ems/plans [ADMITTED]
POST /perf/v1/ems/plans [CHILD_READY]
POST /perf/v1/strategy/signals [ADMITTED]
POST /perf/v1/strategy/signals [CHILD_READY]
```

这些是未来脚本拟采用的统计名称，不是已发布 HyperPerf 场景；创建计划时需与脚本逐字一致。
路径仍为真实 `/perf/v1/...`，方括号只用于统计名称。

```json
{"request_id":"hp-demo-plan-000001","wait_for_child":true}
```

202 表示请求已接纳，不保证响应到达时状态仍为 ADMITTED：worker 可能已使它变为 READY。
不能只检查 HTTP 202 就把 FAILED 结果计为成功。重复 request_id 会复用已有计划，不计新增 TPS。

策略测试事件示例：

```json
{
  "consumer_id": "hp-demo-consumer-000001",
  "scope_id": "perf-scope-0",
  "sequence": 1,
  "event_id": "hp-demo-event-000001"
}
```

同一 consumer 固定到同一 shard、串行递增 sequence，不让多个 worker 竞争一个序号流。
`DUPLICATE` 不能计新增 checkpoint TPS；缺口 409 是失败。本 HTTP 工具的 checkpoint 是
内存测试实现，source 为 `deterministic-contract-feed`，不等于真实 UDS 消费者 FULL 持久化。

### 6.3 HTTP 初始化与错误

| 项目 | 对接要求 |
| --- | --- |
| 数据初始化 | 固定 scope 数、seed 基数、分片数、队列容量、进程/CPU 和本轮 ID 前缀 |
| 服务起停 | 由目标维护者管理，不由 Locust task 启动或重启 |
| 鉴权 | 当前普通性能接口没有应用层鉴权，只适合受控 loopback 目标；新增代理认证另行确认 |
| 429 / 503 | 分别核对背压、暂停、超时、stale 等实际原因，均不能算成功 |
| 查询 404 | fixture 未准备、尚未发布或地址/部署不符，不能回填成功 |
| 422 | 请求合同不符，应先修正客户端或配置 |
| reset / 故障接口 | 第一版不调用；只读指标读取也不能擅自变成 reset |

HTTP 工具内存状态会随写入增长。准备、固定 seed 和清理由负责人单独安排；允许合成数据
不等于允许无限写入。不得在运行时安装 FastAPI、uvicorn 等服务依赖或访问公网补依赖。

## 7 Consumer UDS 协议与调用

### 7.1 帧与会话

协议为 Linux `AF_UNIX/SOCK_STREAM`，不是 HTTP、WebSocket 或裸 JSON。canonical UTF-8
JSON 前有 32-byte 大端 XTC2 header：magic、版本、kind、flags、API/schema version、
correlation ID、payload 长度及 CRC-32。payload 上限 1,048,576 bytes。

复用官方 `ConsumerUnixClientV2`、`ConsumerUnixClientConfigV2` 或经合同回归验证的客户端，
不假设一次 recv 就是一帧。request_id、帧 correlation ID 和业务幂等键是不同概念。

当前配置每连接只处理一个请求，官方小客户端每次 call 建连并在结束后关闭。新 Runner
不能默认长连接无限复用或在同连接上 pipeline；需要变更连接模型时另定 profile。

### 7.2 先执行只读联通

下面示例在服务器**已授权的个人身份**下执行，不是可上传的 HTTP Locust 脚本，不启动靶，
不导入 EMT SDK，也不产生委托。所用解释器要有读取和执行权限：

```bash
/opt/xuanling-tradecore/.venv/bin/python - <<'PY'
from pathlib import Path
from xuanling_tradecore.consumer import ConsumerUnixClientConfigV2, ConsumerUnixClientV2

client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(
    Path('/run/tradecore-consumer-uds/consumer-v2.sock'), timeout_seconds=5.0,
))
request_id = 'hyperperf-readonly-handoff'
reply = client.call({
    'message_type': 'request',
    'schema_version': 2,
    'operation': 'ListAccounts',
    'request_id': request_id,
    'payload': {},
})
if (
    reply.get('message_type') != 'response'
    or reply.get('schema_version') != 2
    or reply.get('request_id') != request_id
    or reply.get('status') != 'COMPLETED'
):
    raise RuntimeError('ListAccounts failed; inspect the returned error code securely')
accounts = reply['payload']['accounts']
if not any(
    a['consumer_account_id'] == 'server-local-uds-benchmark'
    and a['venue_id'] == 'NO_ADAPTER_BENCHMARK'
    for a in accounts
):
    raise RuntimeError('Expected no-Adapter benchmark binding is unavailable')
print('NO_ADAPTER_CONSUMER_READONLY_OK')
PY
```

2026-11-06 以已配置的 `xuanling` 身份执行一次同类 ListAccounts 已返回 COMPLETED，
账户别名和 no-Adapter venue 匹配。它仅证明该身份的只读 UDS 调用，不证明 HyperPerf
沙箱 Runner 的身份、挂载、并发能力或本轮业务写入通过。

### 7.3 SubmitSignal 合同

以下是受控 no-Adapter fixture 的请求示例，**不是本次执行指令**；经放行后每轮替换唯一
前缀并固定测试交易日。绝不能把它改成真实 EMT 账户后沿用相同高并发计划。

```json
{
  "message_type": "request",
  "schema_version": 2,
  "operation": "SubmitSignal",
  "request_id": "hp-demo-submit-000001",
  "payload": {
    "consumer_account_id": "server-local-uds-benchmark",
    "strategy_id": "uds-benchmark",
    "strategy_order_id": "hp-demo-business-000001",
    "trading_day": "2026-11-06",
    "symbol": "TEST",
    "market": "SIM",
    "side": "BUY",
    "price_type": "LIMIT",
    "business_type": "CASH",
    "quantity": 1,
    "protection_price_ticks": 1
  }
}
```

成功外层为 message_type=response、schema_version=2、request_id 匹配、status=ACCEPTED。
payload 含 consumer_request_id、accepted_event_id、oms_order_id、execution_id、journal_id、
journal_lsn。所有 ID 使用真实响应值，不伪造返回样例或用占位值继续查询。

ACCEPTED 是本地 OMS 接纳，不等于 EMT accepted、成交或自动恢复能力。性能证据还需
确认 no-Adapter 路由及真实 Journal/trace，不靠响应里不存在的字段臆测副作用。

超时或断连表示结果不确定，不表示未接纳。重试同一意图时保持 request_id 和
strategy_id/strategy_order_id/规范化意图不变；不自动换业务键。新增 TPS 只统计唯一新事实。

### 7.4 查询操作

| operation | payload | 成功及注意事项 |
| --- | --- | --- |
| ListAccounts | `{}` | COMPLETED，账户和 no-Adapter venue 符合预期 |
| GetOrder | oms_order_id | COMPLETED，payload.order 对应真实回执订单 |
| GetOrderAuditTrail | oms_order_id | COMPLETED，审计指针属于该订单 |
| GetAccountOrderSnapshot | consumer_account_id | 仅适合受字节预算约束的小快照；大账户可能要求分页 |
| CreateOrderSnapshot | consumer_account_id、page_size（1..200） | COMPLETED，固定 snapshot_id、水位、基数和 page_cursor |
| ReadOrderSnapshotPage | page_cursor | COMPLETED，核对 count、orders、has_more、next_page_cursor 和固定 snapshot 信息 |
| CreateReplayPartitions | consumer_account_id、partition_count（1..64），可选 after_cursor | COMPLETED，签发固定水位与分片 |
| PollEvents | cursor、limit（schema 范围 1..10000，实际页受 frame 字节预算约束） | COMPLETED，有序 events、high_watermark_position、has_more、next_cursor |

本轮不测 CancelOrder；SubscribeOrderEvents 的注册合同不能作为 server push 已实现的证明。
上游不得携带内部 account_id、柜台账号密码、client_id 或路由 token。

### 7.5 错误响应

错误 frame 的 JSON 外层为 message_type=error、schema_version=2、request_id、code、retryable，
可有 detail，没有成功 response 的 status。收到一帧不等于成功。

| 常见 code | 排查方向 |
| --- | --- |
| UNAUTHORIZED | 文件 ACL、host peer UID/GID、Consumer/account/strategy binding |
| RATE_LIMITED | 有界接入/接纳容量；计背压，不计成功，不无界重试 |
| NOT_FOUND | seed 或真实回执 ID 不存在 |
| CURSOR_INVALID / CURSOR_EXPIRED | token 被改写、绑定不符、过期或 reset 后使用旧 token |
| SNAPSHOT_REQUIRES_PAGINATION | 改用正式分页，不继续请求全量大响应 |
| SNAPSHOT_NOT_READY / SNAPSHOT_EXPIRED / SNAPSHOT_GENERATION_MISMATCH | 发布状态、生命周期或混用不同快照 |
| FRAME_TOO_LARGE / PAYLOAD_CRC_MISMATCH / SCHEMA_VALIDATION_FAILED | 编码、长度、CRC 或请求字段错误 |
| JOURNAL_GAP / CURSOR_RECOVERY_UNAVAILABLE / AUDIT_EVIDENCE_UNAVAILABLE | 保留证据并停止该项通过判定，不靠本地猜测补齐 |

## 8 分页与事件消费的正确流程

### 8.1 快照

准备阶段建立固定基数数据，调用 CreateOrderSnapshot，记录 snapshot_id、journal_id、
stream_generation、snapshot_position、order_count、page_size、有效期、projection_lag/stale。
正式页查询使用服务端 page_cursor，不自行拼 offset 或篡改签名 token。

区分两种 profile：固定一页重复查询的页 QPS，与遍历完整快照的订单数/整批耗时。
每页 100 条的 2,000 QPS 不是每次返回整个账户；计划必须声明页大小、总基数和读分布。
下一页使用真实 next_page_cursor，直到 has_more=false；总数和唯一性与固定快照对应。

### 8.2 PollEvents

1. 测量窗外预置足量唯一事件并调用 CreateReplayPartitions。
2. 保存 journal_id、stream_generation、event_count、high_watermark_position 和各分片
   partition_id、after_position、high_watermark_position、event_count、cursor。
3. 一个 worker 独占一个 cursor 流；分片区间为 `(after_position, high_watermark_position]`。
4. 原样提交 cursor，验证事件归属、有序、无未解释重复/缺口，检查固定水位。
5. 若声称 checkpointed event/s，客户端必须先原子持久化业务结果与 next_cursor，再计数。
6. has_more=false 时结束该分片。新增事件用已完成 cursor 创建增量计划，不重新从 BEGIN
   循环旧数据。样本耗尽报告 DATASET_EXHAUSTED，不空轮询凑吞吐。

同 token 被多 worker 重读不等于分片消费；event_count 不等于 LSN 差值。cursor 不透明，
不得修改、外发或索要签名密钥。reset 会使旧 namespace、snapshot 与 cursor 失效。

分别报告 PollEvents RPC QPS、received/processed/checkpointed 唯一 event/s，以及 RPC、
响应后 checkpoint、请求到 checkpoint 的逐页延时。没有真实客户端持久化实现时，只能报告
received/processed，不得把返回成功当成 FULL checkpoint。

## 9 HyperPerf 需要确认或补齐的能力

### 9.1 已有 HTTP 运行器约束

- 使用 Locust 2.31.5，HTTP 继承 HttpUser，通过 self.client 发送，显式固定 name。
- host 只有协议、主机和端口；白名单、代理和目标解析均由平台冻结配置控制。
- 并发、速率、预热、窗口、测量及停止宽限来自平台计划，脚本不启动 Locust。
- 当前基线支持 CLOSED 和 CONSTANT_RATE；需要严格发起上限时用 CONSTANT_RATE。
- 运行时不安装依赖、不访问外网、不修改代理，不接受脚本指定 Proxy-Authorization。
- HTTP 和未来 WS 分开脚本、计划及 host。本项目没有已确认的 WS 业务入口，不套 `/echo`。
- WS 基线 exchange 在返回前已计结果；复杂业务校验、推送、关联或自定义握手需扩展确认，
  不能靠脚本重复发送 request event 修正结果。

### 9.2 原生 UDS 接入待交付清单

这是一份需要 HyperPerf 下一阶段回复的能力清单，不是可直接导入的现成 Runner 配置。

| 能力 | 必须明确的合同 |
| --- | --- |
| 插件与场景类型 | extension ID、版本、scenario kind、配置 schema；现有 HTTP/WS kind 不能冒充 UDS |
| socket 授权 | 精确允许的宿主机路径、父目录与 symlink 校验、生命周期和撤销；不全量挂载 `/run` |
| 运行身份 | 服务端实际看到的 host UID/GID，用户命名空间映射及对应 Consumer binding |
| 客户端运行时 | CPython 版本、依赖锁及客户端合同来源，不把服务端 3.10 venv 复制进平台运行时 |
| wire 能力 | XTC2 编解码、CRC、分段读写、长度边界、correlation/request_id、schema 资产校验 |
| 连接模型 | 当前一请求一连接；何时关闭，停止后如何有界排空，不误重试未知结果 |
| 负载调度 | 全局/每操作上限、在途上限、准备与预热分离，发生器延迟和实际释放偏差 |
| 业务结果 | response/error 独立校验，背压、授权失败、超时、连接失败和协议失败分别保留 |
| 指标归属 | 请求只计一次；RPC、事件数量、checkpoint 和批量墙钟分开 |
| 数据与产物 | fixture/namespace/合同摘要、原始时间对、HDR、错误、资源窗口及完整性证明 |

平台运行时与 TradeCore 3.10 的包要求不同，不能默认在 Locust 环境直接安装整个 TradeCore。
由双方选择受控 3.10 客户端角色，或经兼容性回归的独立客户端包；依赖和分发方式需先确认。

### 9.3 如选择 HTTP 到 UDS 测试桥

必须先审查桥接 API，而不是发明 `/submit` 后直接当成既有合同。桥独立进程、仅受控
loopback 接入、严格认证与 payload/方法白名单、有界连接/队列/超时，不转发凭据，禁止任意
socket 路径和通用 RPC 透传。一个桥接 UID 会成为 UDS 的实际 peer，不能伪装原消费者身份。

平台监控须区分发生器、桥和 Consumer 三侧；桥产生的拒绝与 TradeCore 拒绝分开。分别保留
平台到桥、桥到 UDS、TradeCore 服务端 trace，不能用总体 percentile 相减估算子段。
桥接开发、端口和部署均待批准，本次没有交付桥接脚本或服务。

## 10 目标进程与资源监控绑定

### 10.1 绑定正确目标

真实 Consumer 的目标选择器为：

```json
{"type":"SYSTEMD_UNIT","value":"xuanling-tradecore-consumer-uds.service"}
```

这是配置片段，不是完整可导入计划。目标使用 EXTERNAL_MANAGED，由管理员管理生命周期，
HyperPerf 只读解析 MainPID 和启动身份。不要写死 PID，也不要把 `xuanling-tradecore.service`
这个 disarmed runtime 或整个 tradecore.slice 当成本次 Consumer 单进程。

HTTP 组件测试应绑定真正提供对应端口的 HTTP 工具进程；仅测 `/work` 却监控 Consumer，
只能得到空闲 Consumer 的资源，不能证明对其施压。

HyperPerf 的 target-spec selector 与 monitoring-policy selector 枚举并不完全相同，
不能互相复制任意类型；双方优先使用共同支持的 SYSTEMD_UNIT，并按实际 schema 预检。

TARGET binding 参考片段如下；完整 monitoring 配置还必须有 GENERATOR binding 和 sampling，
由平台生成、校验和审批：

```json
{
  "binding_id": "tradecore-consumer",
  "side": "TARGET",
  "process": {
    "selector": {"type":"SYSTEMD_UNIT","value":"xuanling-tradecore-consumer-uds.service"},
    "include_descendants": true,
    "restart_policy": "FAIL_ON_RESTART"
  },
  "threads": {"mode":"ALL","include_future_threads":true,"max_threads":256},
  "profiles": ["PROCESS_RESOURCE", "THREAD_RESOURCE"]
}
```

max_threads 与采样预算要通过部署策略，不因示例写了 256 就自动获准。collection 中按平台
能力配置 collect_target_resources、collect_generator_resources、collect_process_memory_trend；
基础采样建议 1 秒。MEMORY_TREND 等额外 profile 需要插件和部署均声明支持，不能仅凭
全局 schema 允许就添加。CPU_PROFILE、锁/分配剖析及内核全局设置不属于本次联通范围。

### 10.2 资源及缺口语义

| 范围 | 建议保留 |
| --- | --- |
| 进程 | PID/start identity、CPU user/system 及消耗核数、RSS、可读时 PSS、FD、线程数、I/O、缺页、上下文切换 |
| 线程 | TID/start identity、名称、CPU、运行状态、迁核及上下文切换 |
| cgroup | memory.current/peak、OOM、CPU throttling、CPU/内存 PSI |
| 发生器 | CPU、内存、在途、发起槽位延迟、任务/连接丢弃、退出状态 |
| 服务 | 队列/连接高水位、trace 丢弃、Journal 余量、投影/消费 lag |

CPU 1 core 等价约 100% 单核占用，应明确是否按整机归一化。父进程 RSS 不是服务总内存，
多个进程共享映射的 RSS 不能盲目求和；cgroup 文件缓存也不能直接当成泄漏。

Agent 要有精确 selector、/proc 和 cgroup 的只读授权。缺权限、PID 复用、线程消失或
采样丢失必须报告 gap/缺证，不补零。一次普通性能轮发生目标重启即中断本轮通过判定；
若另外开展重启测试，才采用经批准的分段策略。

## 11 测量口径与负载边界

### 11.1 必须分别计数

| 指标 | 正确分子与完成边界 |
| --- | --- |
| HTTP offered / completed | 实际发起 / 校验成功响应；不是配置 target_rate |
| OMS fact TPS | 唯一事实完成 PROJECTED 的数量 |
| EMS admission TPS | 新 request_id 的有效接纳，不混入幂等重放 |
| EMS child READY TPS | 实际子指令准备完成，不用 202 接纳代替 |
| UDS Submit TPS | 唯一新 OMS intent 的有效 ACCEPTED，不含拒绝和重复业务键 |
| Snapshot/订单 QPS | 明确是单订单、固定页还是全快照响应完成 |
| PollEvents RPC QPS | 成功分页请求次数，不是页内事件数 |
| checkpointed event/s | 实际完成客户端持久化的唯一事件数 |

持续速率、瞬时并发和一次性同时释放不是同一指标。2,000 users 不表示共同释放
2,000 个请求；同时释放验收需要正式 burst 调度能力，不能用高 spawn_rate 冒充。

当前接入讨论的上限是**总并发不超过 2,000、合计请求发起不超过 2,000 次/秒**。
混合例子是 Submit 1,000 RPC/s 与查询 1,000 RPC/s，不是每个操作各 2,000 后叠加。
后续消费可因每页多条而有不同 event/s，需另列数据基数和目标，不能混算。

历史标准的独立 2,000 TPS + 2,000 QPS 或更高 profile 不自动成为本次授权，也不因本轮
限速而被改写为已通过。场地 EMT 500 笔/秒边界独立存在，本次 no-Adapter 不占用其额度。

### 11.2 延时与证据

UDS trace 按原标准逐笔保留 U0（发送前）、U0A（发送返回）、U1（首字节）、U2（完整帧）、
U3（校验/授权通过）、U4（本地 Journal/receipt 完成）、U5（响应发送）、U6（消费者校验完成）。
重点区间为 U0→U6、U1→U2、U2→U3、U3→U4、U4→U5、U2→U5。

当前官方小客户端没有承诺直接导出全部点位；Runner、服务端 trace 和关联产物需要正式
装配。只有总 RPC 延时不能声称已补齐内部拆段。frame 发送前的建连和排队另行计时，
共同 release 至最后一笔完成的批量墙钟也单独报告。

不能相加 percentile、跨轮池化、用两个总体分位数相减推导某段延时，或用 HTTP RTT
冒充 C0–C7。平台统一微秒单位不代表微秒级准确度；CPU 采样窗口也不代表逐请求 CPU 归因。
第一版资源关联按窗口处理，不自动声明 L3_REQUEST。

如果正式报告出现 HDR_DISTRIBUTION_NOT_MERGED 或分位数为空，保留原始 HDR/CSV 和
缺口说明；只有可信的实际计算结果才能填 P50/P95/P99/P100，不生成替代数据。

## 12 初始化 清理与执行流程

### 12.1 数据与 reset

合成数据必须使用 run/shard/worker/序号唯一前缀。写入前固定 fixture、交易日、种子数量、
Journal 实际容量和持久化语义。每订单可能产生多个 fact，失败/幂等路径也要计容量。

独立轮次在窗口外由维护者确认 clean namespace 或受控 reset，记录前后 PID、启动时间、
namespace/Journal ID 与 cursor 失效。不能只写一句“已 reset”，也不能假设任何重启都自动
清空状态。常驻/长期 profile 的重启和恢复语义需单独确认。

第一版发生器不携带 sudo，不删除 Journal，不重启服务。后续如需测试员自助 reset，由
管理员提供精确白名单的受控操作；不能授予通用 shell 或让脚本递归删除状态目录。

### 12.2 分阶段交付与通过标准

所有执行阶段仍需负责人放行。本表描述交接后的顺序，不是本次运行指令。

| 阶段 | 开发与执行内容 | 通过证据 |
| --- | --- | --- |
| J0 目标冻结 | 核对双方版本、目标模式、端口/socket、身份、资源和允许范围 | manifest、配置摘要、选定合同，不能仍把 18180 的 404 当可测目标 |
| J1 平台联通 | 已批准 HTTP 脚本以 1 user、1 request/s 低负载运行 | 非零真实样本、200/accepted=true、零未解释错误、原生产物齐全 |
| J2 HTTP 组件 | 部署/登记经批准的隔离工具，准备 fixture，逐接口合同冒烟 | 每接口完整请求/响应、真实成功判断；不是只验证 /health |
| J3 监控接入 | 绑定实际 TARGET 和 GENERATOR，检查线程与 cgroup 范围 | 非空资源序列、身份正确、权限/缺口清楚，目标确实随本轮负载工作 |
| J4 UDS 前置 | HyperPerf 新能力可用，绑定正确 host peer 身份，只读 ListAccounts | 从正式 Runner 发出的真实 UDS 成功，不是在开发者 WSL 自建 listener |
| J5 UDS 功能包 | 经批准少量 Submit、GetOrder、分页、分片回放与幂等 | 真实唯一事实/回执、读写匹配、无越权；checkpoint 仅在确实持久化后计数 |
| J6 低负载联合 | 先 1，再 10、50 档；实际速率与资源同时采集 | 样本完整、业务及异常计数一致、目标与发生器均健康 |
| J7 性能验收 | 另行审批 200/500/1,000/2,000 档和混合，固定版本完成独立三轮 | 全部按本轮 profile 判定，吞吐/延时/资源/完整性逐项给出，不凭旧结果放行 |

首轮可申请 1 user、spawn_rate=1、target_rate=1、0 秒预热、10 秒测量、5 秒停止宽限；
这些值只在平台计划配置。max_in_flight 是否被实际 Runner 执行须由平台说明，不能只填
字段就宣称具备该限制。没有负责人对新脚本/计划的放行，不复用旧计划授权启动。

## 13 双方职责与交付清单

| 负责人 | 需要交付 |
| --- | --- |
| TradeCore 维护者 | 被测服务/fixture 和合同、no-Adapter 证明、身份 binding、已安装版本证据、Journal 容量及受控清理、必要服务端 trace |
| HyperPerf 开发 | 下一阶段版本和 Runner 合同、UDS 或经批准桥接接入方案、运行身份与隔离、依赖锁、目标环境/白名单、指标/停止/产物能力 |
| 服务器管理员 | 个人登录与 Git 权限、最小监控权限、受控 socket 授权、端口协调、CPU/内存隔离和必要 reset 操作 |
| 联调开发/测试 | 使用公开接口的脚本、明确业务断言、数据/幂等/分页/checkpoint、低负载冒烟与原始证据、按合同提交问题 |

下一位开发开始写接入代码前，请返回：

- [ ] HyperPerf 实际 deployed commit、Locust 扩展版本及下一阶段是否支持 UDS。
- [ ] 原生 UDS 或测试桥的正式选型；桥接 API 不能由脚本侧自行假设。
- [ ] Runner 的 host UID/GID、namespace 映射、socket 可见范围及权限方案。
- [ ] 实际 HTTP origin、占用端口归属、scope/seed/进程分片参数和新环境版本。
- [ ] TARGET/GENERATOR 的 selector、授权监控 profiles、采样预算和缺口行为。
- [ ] 客户端运行时、离线依赖锁、请求计数与有界停止合同。
- [ ] 新脚本/计划的低负载执行窗口与负责人放行记录。

每轮交付 profile/manifest、实际版本与脚本/合同 hash、阶段和负载配置、数据基数、
实际计数、延时、错误、资源序列、trace 覆盖及证据 SHA-256。未知、未跑、缺证与失败
分别写清，不用一个总 PASS 掩盖未覆盖部分。

原生产物受控保留，不手工改 CSV/JSON/HDR。向其他团队转发时另外生成脱敏摘要，去除
凭据、真实账户/标的/价格/订单号、完整 cursor 和私有 payload；不覆盖原始证据。本文中的
服务器地址、路径和测试别名只面向内部授权联调人员，不作为公开发布材料。

## 14 常见问题排查

| 现象 | 优先检查 |
| --- | --- |
| SSH Permission denied | Linux 登录是个人账号和 22；Git 是 git 用户和 2222；由管理员核对密钥/账号，不共享管理员密码 |
| /work 正常但 TradeCore CPU 不变 | 请求还在示例靶，或 TARGET selector 绑错 |
| 18180 有监听但 health 404 | 占用方/路径不是目标合同；本次已观察到此问题，不能直接发业务压测 |
| UDS 路径在沙箱不存在 | 尚未提供受控挂载/Runner，不自行访问宿主路径逃逸 |
| 能 stat socket 却无法调用 | SO_PEERCRED、主 GID、binding 与组文件权限是不同条件 |
| 收到响应却业务失败 | 检查 message_type/error code 或 HTTP JSON，不只统计连接成功 |
| 并发 PollEvents 重复事件 | 多 worker 复用同 cursor、循环 BEGIN，或 checkpoint/分片归属错误 |
| 快照变大后混合负载变慢 | 检查是否请求全量大账户，固定 page_size/快照基数并使用分页 |
| 第二轮越来越慢 | 检查数据增长、Journal 余量、过期快照/cursor、维护和共享 I/O，不直接归因客户端或背压 |
| 监控显示 0 | 检查采集权限与身份/gap，缺证不是 0 用量 |
| git pull 后行为未变 | 已安装 wheel 未更新，或运行的是其他目录/进程 |

## 15 代码与标准索引

TradeCore 路径均相对 Python 项目根目录：

| 内容 | 路径 |
| --- | --- |
| HTTP 联通脚本及说明 | `tools/hyperperf/http_smoke/` |
| 暂停存档 | `tools/hyperperf/ARCHIVE.md` |
| OMS HTTP 工具 | `tools/oms_load_platform/server.py`、`service_v1.py`、`README.md` |
| EMS/策略 HTTP 工具 | `tools/ems_strategy_load_platform/server.py`、`service_v1.py`、`README.md` |
| 官方 UDS 小客户端 | `xuanling_tradecore/consumer/client_v2.py` |
| wire/解码/peer | `xuanling_tradecore/consumer/wire_v2.py`、`unix_v2.py` |
| 操作分发与响应字段 | `xuanling_tradecore/consumer/dispatch_v2.py` |
| no-Adapter 启动与配置 | `xuanling_tradecore/consumer/uds_server_v2.py`、`deploy/consumer-uds/` |
| Consumer schemas | `contracts/consumer/v2/schemas/`，含 request/response/event/error schema |
| 合同摘要及兼容矩阵 | `contracts/consumer/v2/schema-hashes.json`、`compatibility-matrix.json` |
| 已有发生器参考 | `tools/performance/consumer_uds_steady_probe.py`、`consumer_uds_mixed_probe.py`、`consumer_uds_replay_probe.py` |

已有发生器是诊断/验收工具，不是可原样上传的 HyperPerf Locust 场景；它们的私有配置读取、
运行时、调度及产物逻辑要经平台正式接入，不能通过随意 subprocess 包装绕过约束。

HyperPerf 源码入口：

```text
test-targets/http/server.py
test-targets/http/load_test.py
test-targets/websocket/load_test.py
extensions/locust-runner/schema.json
extensions/locust-runner/src/hyperperf_locust_runner/configuration.py
extensions/locust-runner/src/hyperperf_locust_runner/locust_main.py
extensions/locust-runner/src/hyperperf_locust_runner/websocket.py
contracts/schemas/target-spec.schema.json
contracts/schemas/run-snapshot.schema.json
contracts/schemas/monitoring-policy.schema.json
execution-agent/src/hyperperf_agent/runtime/external_target.py
execution-agent/src/hyperperf_agent/runtime/network_package.py
execution-agent/src/hyperperf_agent/runtime/monitoring_deployment.py
execution-agent/src/hyperperf_agent/runtime/process_thread_sampling.py
execution-agent/src/hyperperf_agent/runtime/monitoring_evidence.py
```

进一步依据：

- [统一性能压测标准](../05-统一性能压测标准.md)。
- [ConsumerGateway V2 上游接口规范](../../02-接口与数据/06-ConsumerGatewayV2上游策略联调接口规范.md)。
- [真实 UDS 压测标准](11-真实UDS-Consumer-V2压测与验收标准.md)。
- [服务器 UDS 交接](12-服务器真实UDS部署与压测方交接.md)。
- [PollEvents 分片与 checkpoint](14-PollEvents签名分片游标与UDS-S2-5K联调说明.md)。
- [生产 wheel 与源码版本边界](27-生产版本与性能基线-2026-10-28.md)。

历史文档中的容量、权限和“已通过”只适用于其指定版本/拓扑，不覆盖本次 HyperPerf 集成。
对接完成后，分别给出平台联通、HTTP 组件、真实 Consumer、资源监控和性能验收的结果，
不能用其中一项通过替代其余四项。
