# EMT 受控模拟盘变更压测

`gateway-mutation` 是网关可实际执行的受控变更链路验证：它支持普通、信用和期权模拟账户的限价委托/撤单周期，每周期对该返回委托号撤单，并在每个变更后执行 `QueryOrderByEMTID` 回查。它验证的是命令 C0–C7、回调 Journal、ACK 和查询回查的端到端路径，不是吞吐压测，也不构成生产 SLA。

默认仅预检，绝不加载 SDK、登录或调用 EMT。真实执行必须同时满足：本地 mutation fixture 已 `armed: true`、仅启用 `InsertOrder` 与 `CancelOrder`、两者速率均为每秒 1 次、回查接口均为 `QueryOrderByEMTID`，并在命令行显式增加 `--execute-simulated-mutations`。

## 本地配置

先生成不含账号、密码、地址和订单参数的默认模板：

```bash
.venv/bin/emt-gateway-perf --driver mutation-fixture-template --output ./data/performance/fixtures/emt/emt-mutation-fixture.json
```

仅在 Git 忽略的本地 fixture 中设置所选账户的两项：

- `InsertOrder`：`enabled: true`，填写显式限价单字段；普通账户只允许六位代码、限价单、普通现金业务（`business_type: 0`、`side: 1/2`）、数量 1–100。
- `CancelOrder`：`enabled: true`，`request` 必须为空；网关只会撤销本次刚返回的委托号。

两项都必须为 `rate_limit_per_second: 1`，并设 `reconciliation_query: "QueryOrderByEMTID"`。`order_client_id` 由运行时随机生成，禁止写入 fixture；账户、凭据、柜台地址、回调 payload 也禁止写入。

### 融资融券限价单

信用账户可以用同一受控闭环验证真正的融资融券委托：`business_type: 4`，且只能使用 `side: 21`（融资买入）或 `side: 22`（融券卖出）。每次运行仅允许一个 100 股限价下单/撤单周期，命令间隔不少于一秒，任何拒绝、成交、回调超时或回查异常都会立刻停止，绝不自动重试。

执行前必须先在同一信用模拟账户完成 `QueryCreditTargetStkByPage`、`QueryCreditMarginRate`，并人工确认标的是当日可融资/可融券标的；融券卖出还必须确认券源可用，且限价不得低于最新成交价。上述市场状态不能写入报告或提交到仓库。若委托可能成交，不得执行该基准测试；应使用经人工确认的非成交限价并随后撤单。

## 预检与实测

下面的预检会校验全部限制，但不会加载 SDK 或登录：

```bash
.venv/bin/emt-gateway-perf --driver gateway-mutation --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --mutation-fixture ./data/performance/fixtures/emt/emt-mutation-fixture.json --profile ordinary --output ./artifacts/performance/gateway-mutation-preflight.json
```

确认预检报告为 `executed: false` 后，才使用以下命令执行一次模拟盘订单周期。批量模式以 `--mutation-cycles 10` 为上限；每一个原生下单或撤单命令之间至少相隔一秒，任一命令、回调或回查失败会立即停止，绝不发送后续周期或自动重试：

```bash
.venv/bin/emt-gateway-perf --driver gateway-mutation --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --mutation-fixture ./data/performance/fixtures/emt/emt-mutation-fixture.json --profile ordinary --interval 1 --execute-simulated-mutations --output ./artifacts/performance/gateway-mutation-ordinary.json
```

融资买入或融券卖出均使用 `--profile credit`，且必须只执行一个周期：

```bash
.venv/bin/emt-gateway-perf --driver gateway-mutation --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --mutation-fixture ./data/performance/fixtures/emt/emt-mutation-fixture.json --profile credit --mutation-cycles 1 --interval 1 --execute-simulated-mutations --output ./artifacts/performance/gateway-margin-order.json
```

10 周期受控复测：

```bash
.venv/bin/emt-gateway-perf --driver gateway-mutation --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --mutation-fixture ./data/performance/fixtures/emt/emt-mutation-fixture.json --profile ordinary --mutation-cycles 10 --interval 1 --execute-simulated-mutations --output ./artifacts/performance/gateway-mutation-ordinary-10.json
```

每次真实运行都会在报告旁写入同名 `.traces.jsonl`，其中只有 C0–C7、接口、档案、workload、结果和白名单指标。多个批次可离线聚合，以得到精确的跨批次直方图和每场景样本门槛状态：

```bash
.venv/bin/emt-gateway-perf --driver gateway-mutation-aggregate --mutation-trace-input ./artifacts/performance/batch-a.traces.jsonl ./artifacts/performance/batch-b.traces.jsonl --output ./artifacts/performance/gateway-mutation-aggregate.json
```

运行时不自动重试。命令 callback 由单一常驻消费者持续写入 Journal 并 ACK，即使执行器正等待回查或限速，也不会把后续订单通知滞留在 ring 中而误计为 callback 延迟。如果下单未获得完整 C0–C7 回调，或下单后的回查未成功完成，流程立即停止且不会继续撤单。报告和 Journal 只记录接口名、阶段时间、结果、数值型本地错误码和 hash；不写入凭据、证券代码、价格、数量、委托号或业务回包。

资金划拨、信用还款、展期、期权行权和组合操作仍由各自独立的最小影响 fixture 驱动；不得把它们混入本订单/撤单链路。
