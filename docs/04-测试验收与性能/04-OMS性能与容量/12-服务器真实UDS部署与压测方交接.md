# 服务器真实 Consumer V2 UDS：部署与压测方交接

## 1. 当前服务器 target

TradeCore 运营方已在 Linux 服务器部署独立、无 Adapter 的 Consumer V2 Listener：

```text
独立 generator（同一 Linux 主机、Unix UID/GID 1000/1000，即 xuanling）
  → AF_UNIX / SOCK_STREAM
  → /run/tradecore-consumer-uds/consumer-v2.sock
  → SO_PEERCRED principal binding
  → XTC2 decode + schema + ConsumerGateway V2
  → MmapOmsFactJournalV1 commit
  → IntentReceipt
```

服务名称为 `xuanling-tradecore-consumer-uds.service`，已设置开机自启。它与
`xuanling-tradecore.service`（DISARMED runtime）和 fault supervisor 相互独立；压测方
不得停止、重启或替换其他服务。经授权的 `xuanling` 压测账户可在独立轮次之间使用
`sudo -n /usr/bin/systemctl restart xuanling-tradecore-consumer-uds.service`
对本 UDS 服务执行受控 reset；测量窗口内禁止重启。

该 target 不导入、不启动、也不调用 EMT/QMT/EMS/Adapter。成功回执只证明：

```text
journal_first = true
gateway_dispatched = false
```

它不是柜台报单、成交、callback 或场地吞吐证据。

## 2. 已完成的部署验收

| 项目 | 结论 |
| --- | --- |
| Linux CPython | TradeCore 专用 CPython 3.10 `.venv` 已用于服务进程。 |
| 实际 UDS | 服务端已实际 bind `AF_UNIX/SOCK_STREAM`，不是 WSL fixture，也不是 TCP loopback。 |
| 权限 | 父目录为服务账户私有 `0700`，socket 为 `0600`；路径不应被复制、映射或跨主机访问。 |
| 身份 | `SO_PEERCRED` 绑定 UID/GID `1000/1000`。非授权 UID 在 socket 边界拒绝，未进入 OMS。 |
| 持久化 fence | `SubmitSignal` 的回执已在服务端返回可验证的 mmap journal LSN；事实完整写入 frame、CRC 校验、最后写 commit word。 |
| 副作用 | 无 Adapter dispatch；现有 TradeCore DISARMED runtime/fault supervisor 保持运行。 |

该 mmap journal 是文件支持的 OS page-cache **进程崩溃 fence**：不在热路径调用
`fsync`/`msync`，不能被解释为掉电级持久化保证。

## 3. 压测方必须遵守的连接方式

压测程序必须运行在**同一台 Linux 服务器**，并使用已授权的本地 Unix principal
`xuanling`。它只作为 client 连接现有 socket，绝不 `bind()`、删除、chmod 或自行创建
`consumer-v2.sock`。

```python
from pathlib import Path
from xuanling_tradecore.consumer import ConsumerUnixClientConfigV2, ConsumerUnixClientV2

client = ConsumerUnixClientV2(
    ConsumerUnixClientConfigV2(
        socket_path=Path("/run/tradecore-consumer-uds/consumer-v2.sock"),
        timeout_seconds=5.0,
    )
)
```

如果压测程序实际运行用户不是 `xuanling`，它应被拒绝。这是正确安全结果，不允许通过
把 socket 改成 `0666`、放宽父目录权限、关闭 `SO_PEERCRED` 或在 WSL 新建 listener
绕过。应由 TradeCore 运营方先变更 root-owned binding profile，重新完成 P2/P3 验收。

## 4. 最小有效请求

下面的值仅适用于无 Adapter benchmark binding；不包含柜台账号、密码、交易地址、真实
标的、价格或订单标识。每个新样本必须替换 `request_id` 和 `strategy_order_id` 为唯一、
不可逆的脱敏值，避免幂等命中。

```python
request = {
    "message_type": "request",
    "schema_version": 2,
    "operation": "SubmitSignal",
    "request_id": "<unique-redacted-request-id>",
    "payload": {
        "consumer_account_id": "server-local-uds-benchmark",
        "strategy_id": "uds-benchmark",
        "strategy_order_id": "<unique-redacted-business-key>",
        "trading_day": "YYYY-MM-DD",
        "symbol": "UDS-BENCHMARK",
        "market": "SIM",
        "side": "BUY",
        "price_type": "LIMIT",
        "business_type": "CASH",
        "quantity": 1,
        "protection_price_ticks": 1,
    },
}
response = client.call(request)
assert response["status"] == "ACCEPTED"
```

`ListAccounts` 可用作压测前的只读连接检查。不得将 `GetAccountOrderSnapshot`、
`PollEvents` 的种子写入吞吐测量窗口；按
[真实 UDS Consumer V2 压测与验收标准](11-真实UDS-Consumer-V2压测与验收标准.md)
分别执行。

## 5. 正式压测开始前的分工

| 角色 | 负责事项 |
| --- | --- |
| TradeCore 运营方 | 保持 service active；提供仅限本 UDS unit 的重启授权；保留旧 mmap journal 作为 evidence；核验没有 Adapter 进程。 |
| 压测方 | 每轮测量前自助受控 reset 并记录前后 PID/启动时间；以独立 client/generator、独立资源采集器连接现有 socket；按标准生成脱敏 `manifest.json`、原始时间对、HDR、资源数据、错误分类与校验和。 |
| 双方 | 仅将 server-local `AF_UNIX/SOCK_STREAM` 结果作为 UDS 基线；不外推成 EMT/QMT/EMS/柜台延时。 |

## 6. 尚待压测方产出的正式 evidence

当前无 Adapter UDS target 采用显式 `listen(4096)` 和非阻塞请求 reactor；最多 2048 个完整
请求等待处理、4096 个未完成首帧连接、6144 个总连接。已取消该 target 的 16 个连接 worker。
每次调度最多处理 64 笔；慢读、半包和保持打开的连接不占用业务 worker。授权、CRC/schema、
OMS 幂等/风险检查和 Journal commit 后回执均沿用现有逻辑，单连接保持请求顺序。
完整请求在进入 OMS 前遇到队列满时，经有界响应队列返回 `RATE_LIMITED` 和
`INGRESS_OVERLOADED_PRE_DISPATCH`。未收齐首帧或拒绝响应队列也满时仍可能关闭连接；
客户端没有收到明确响应时必须保留未知结果并对账，不能仅凭客户端异常认定未执行。
背压拒绝不能算作 `ACCEPTED`，也不能通过扩大等待窗口计入窗口内吞吐。
日志中 `worker_queue_wait_ns_*` 在此模式表示“完整请求就绪→开始处理”等待；测试 profile
需更新为 `request-reactor`，不能继续声明 `connection_workers=16`。
**诊断复测不能替代测试方正式三轮和逐请求 server trace 验收。** 内核实际
backlog 上限以目标主机 `somaxconn` 和 `ss -xlpn` 的 `Send-Q` 为准；批量连接成功
也不等于批量业务受理。`UDS-BP-1` 应同时保留连接、回执、超时三个独立计数。
压测发生器的 `RLIMIT_NOFILE` 软上限必须至少为并发数加 64；若仍为 1024，约
千笔后出现的 `EMFILE` 是**客户端限制**，不能归因于服务端。每次 Git 拉取后还必须
将该 checkout 安装到服务使用的 `.venv`，否则 systemd 仍运行旧 wheel。

### 2026-09-24 服务端突发探针（非正式性能验收）

服务器从 `dev` 拉取并安装服务端改造后，`ss -xlpn` 实测 listener `Send-Q=4096`；
200 笔同时释放为 200/200 建连、200/200 `ACCEPTED`、200 个不同 Journal LSN，
整批墙钟 124.44 ms。第一次 2000 笔试跑仅 1018 建连，查明是发生器的
`ulimit -Sn=1024`；提升至 8192，并在每轮前受控 reset 后，得到：

| 独立轮次 | 建连/发出/ACCEPTED | 唯一 Journal LSN | P50 / P95 / P99 / P100（ms） | 整批墙钟（ms） |
| --- | --- | ---: | --- | ---: |
| 1 | 2000 / 2000 / 2000 | 2000 | 892.74 / 2240.66 / 2368.06 / 2400.34 | 2419.65 |
| 2 | 2000 / 2000 / 2000 | 2000 | 751.30 / 1539.68 / 1620.36 / 1637.68 | 1660.53 |
| 3 | 2000 / 2000 / 2000 | 2000 | 864.04 / 2265.13 / 2426.93 / 2466.43 | 2484.63 |

脱敏逐笔时间证据保留在服务器 `/var/tmp/tradecore-uds-burst-2000-*-20260924.ndjson`，
每轮 2000 行；探针脚本是 `tools/performance/consumer_uds_burst_probe.py`。
三轮文件按表格顺序的 SHA-256 分别为
`458815bb7254f87ddc3705766297a4c97743dec47286f46eb74ef18ff50fbd7f`、
`f54ab04d0756892649a2db37a5ed08f914248cba27395fe4285decb0ee5b6fab`、
`32edfd834c9e0f27decb787e1a86fb6ec2c66f9fbecdb4c5105e70dad5e0fbaf`。
最终 UDS unit 为 `active`、`NRestarts=0`。这些结果只证明 **2000 笔突发全部
接入并产生 2000 个不同的 OMS Journal 回执**；不是 2000 accepted TPS 的持续
30 秒证明，也缺少正式 U0–U6/HDR/资源证据，不能标作统一标准验收通过。

服务器部署和 Linux socket smoke 已完成；下列需要真实独立 generator 连接本文 socket 后，
按统一标准执行，不能用部署冒烟替代：

1. U0–U6 完整 trace 关联、采样覆盖与 HDR 结果；
2. `UDS-HOT-1` 三轮 10,000 热态样本；
3. `UDS-S1-2K`、`UDS-S3-5K`、`UDS-S2-5K`、`UDS-S5-MIX`、`UDS-BP-1`；
4. 独立复核、资源/错误 evidence 与 SHA-256 清单。

在这些 evidence 完整前，状态只能是“真实 server target 已部署”，不能写作“UDS 性能
验收通过”。
