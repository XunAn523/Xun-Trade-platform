# EMT 模拟盘 Consumer 即用调用说明

## 当前可用入口

2026-10-31 已在服务器 `192.168.251.1` 部署并验证真实模拟盘链路。此入口仅限操作员指定的 EMT 普通模拟账户，不是实盘 READY 或自动恢复证明。旧的 no-Adapter 压测服务没有被替换。

| 项目 | 当前值 |
| --- | --- |
| systemd 服务 | `xuanling-tradecore-emt-simulator.service` |
| 同机 UDS | `/run/tradecore-emt-simulator/consumer-v2.sock` |
| 客户端 Python | `/opt/xuanling-tradecore-emt-simulator/.venv/bin/python` |
| Consumer 账户别名 | `emt-sim-ordinary` |
| 策略 ID | `consumer-sim-strategy` |
| SDK 返回交易日 | `2026-10-31`；不要按客户端本地日期猜测 |
| 账户状态 | `SIMULATOR_ACTIVE`，不是 `READY` |
| 当前允许的指令 | `000001`、`SZ`、`CASH`、`LIMIT`、每单 `1–100` 股；券商仍执行自己的校验 |
| 价格单位 | 人民币分，`1039` 表示 `10.39` 元 |
| 撤单 | 禁用；`CancelOrder` 返回 `UNAUTHORIZED` |
| 数据面传输 | 服务器同机 AF_UNIX/SOCK_STREAM，使用 Consumer V2 客户端编码 XTC2 帧，不是 HTTP、WebSocket 或裸 JSON |
| 身份 | Linux `SO_PEERCRED`；当前授权 xuanling、fengyu、yangxue 的配置身份，且必须有 socket 文件访问权限 |

账户、密码和柜台地址只由服务端 systemd 凭据及受控配置加载。上游不传这些字段，不导入 EMT SDK。

## 最小调用示例

代码必须在该 Linux 服务器执行。其他主机不能直接连接这个 UDS 路径。示例没有启动服务或创建测试 UDS，连接的是上表已部署的真实模拟盘服务。

```python
from pathlib import Path
from xuanling_tradecore.consumer import (
    ConsumerUnixClientConfigV2,
    ConsumerUnixClientV2,
)

client = ConsumerUnixClientV2(
    ConsumerUnixClientConfigV2(
        Path("/run/tradecore-emt-simulator/consumer-v2.sock"), 5
    )
)

def call(operation, payload, request_id):
    return client.call({
        "message_type": "request",
        "schema_version": 2,
        "operation": operation,
        "request_id": request_id,
        "payload": payload,
    })

print(call("ListAccounts", {}, "check-account"))

# 运行下面这条调用会真的向 EMT 模拟账户发单，不是 mock。
# 同一业务委托的 strategy_order_id 必须固定；超时重试也不能换业务键。
receipt = call("SubmitSignal", {
    "consumer_account_id": "emt-sim-ordinary",
    "strategy_id": "consumer-sim-strategy",
    "strategy_order_id": "YOUR-UNIQUE-BUSINESS-ORDER-ID",
    "trading_day": "2026-10-31",
    "symbol": "000001",
    "market": "SZ",
    "side": "BUY",
    "price_type": "LIMIT",
    "business_type": "CASH",
    "quantity": 100,
    "protection_price_ticks": 1039,
    "limit_price_ticks": 1039,
}, "submit-request-1")
print(receipt)

if receipt.get("status") == "ACCEPTED":
    order_id = receipt["payload"]["oms_order_id"]
    print(call("GetOrder", {"oms_order_id": order_id}, "read-order-1"))
```

`SubmitSignal: ACCEPTED` 只代表 OMS 本地事实已提交，不代表柜台最终受理。必须继续查询，或用标准 `CreateReplayPartitions` / `PollEvents` 获取回报。`WORKING` 表示已收到工作中状态，不代表成交。

```python
plan = call("CreateReplayPartitions", {
    "consumer_account_id": "emt-sim-ordinary",
    "partition_count": 1,
}, "event-plan")["payload"]

for part in plan["partitions"]:
    cursor = part["cursor"]
    while True:
        page = call("PollEvents", {"cursor": cursor, "limit": 100}, "poll-page")["payload"]
        for event in page["events"]:
            print(event)
        cursor = page["next_cursor"]
        if not page["has_more"]:
            break
```

分片是创建计划时的固定水位视图。下一次查询新增事件时，用已验证完成的 `next_cursor` 作为下一轮 `CreateReplayPartitions` 的 `after_cursor`；不要从 BEGIN 重建后当成新增消费，也不要拆改签名令牌。当前进程的 cursor 签名 key 随该实例生存，不是跨重启恢复合同。

## 已验证证据

真实测试由独立客户端经公开 UDS 接口完成，客户端没有导入 native SDK：

- 一笔 `000001` 买入 100 股、限价 10.39 元，收到 `ACCEPTED → WORKING`。
- 相同业务键重发返回同一 OMS/执行标识。
- `PollEvents` 读到该执行的 `WORKING` 回报。
- 只读 native Journal 核对：`ORDER_PRE_NATIVE=1`、`ORDER_CALL_STARTED=1`、`ORDER_NATIVE_ACCEPTED=1`，没有第二次 native 报单；Journal 校验通过。
- 撤单 Journal 为零；没有撤单或账户级清理。本次复核时该委托仍为 `WORKING`，不能标为已成交或已清理。
- `fengyu` 已从真实客户端连接新 UDS 并完成 ListAccounts / Snapshot 查询；只增加了 `tradecore-testers` 组成员权限，没有增加通用 sudo。

服务器脱敏证据位于：

```text
/var/lib/xuanling-tradecore-emt-simulator/g-e0a1549-20261031/consumer-smoke-20261031T1001Z.json
/var/lib/xuanling-tradecore-emt-simulator/g-e0a1549-20261031/consumer-smoke-native-proof.json
```

以上是一笔真实模拟盘穿透验证，不是当前服务的 499 性能基线、持续压测或完整恢复验收。启动相关代码来自 Git `dev` 的 `e0a1549e13594accba132f4be918a2ffb28f4873`；后续文档或测试提交不等于运行包已被更新。

## 运行与边界

该服务使用独立 `.venv`、独立 SDK 副本、root 控制的 profile/签名/授权文件和 systemd 单账户凭据。普通路径的恢复门禁没有降低，未验证的唯一性/echo probe 标志仍为 false；模拟路径依靠操作员指定的账户与 namespace，不宣称跨系统、跨主机或柜台全局唯一性已经证明。

当前实例容量为 10,000 个执行；队列有界，单 native owner 保守按约 476 次/秒排队，柜台仍受账户总计 500 次/秒额度约束，其他系统的同账户动作也计入。不要把本地接纳 TPS 当作柜台 TPS。该模拟实例没有重新验收低延时或高并发 SLA。

```bash
systemctl status xuanling-tradecore-emt-simulator.service --no-pager
```

该实例不支持直接重启后自动放行：`started-once`、namespace 状态和 Journal 会保留，异常退出或重复启动不会以清空历史的方式重报。不要删除这些文件，也不要把旧业务订单换新键补发。操作员需先核对已有模拟委托，再准备新 generation。

签名 namespace 绑定交易日。部署配套的 day-fence timer 在 `2026-11-01 00:00:00 Asia/Shanghai` 停止本实例，不撤销已挂委托；新的交易日需重新查询 SDK 交易日并准备签名 generation。服务已 enabled，但开机尝试启动不等于自动重新授权已有 generation。
