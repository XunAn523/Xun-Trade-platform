# EMT 原生只读基准

`native-readonly` 连接真实 EMT 测试柜台，但它只登录并顺序执行查询。当前覆盖资产、资金、持仓、委托、成交、IPO、配股及昨日资产等 10 个查询；不包含下单、撤单、资金划转、信用还款、行权或组合操作。

每个请求先记录 C0，再分别记录原生调用返回的 C1 与最终查询回调 C5。默认每次请求间隔 250 ms、每个回调最多等待 8 秒，因此这是低频真实性基线，不是未经券商批准的高并发冲击测试。驱动固定使用 EMT 的 `FATAL` 日志级别，以避免供应商的 `INFO` 日志输出登录参数。

在根目录的 CPython 3.10 `.venv` 内执行。账户、密码和柜台地址必须来自本机密钥服务或当前进程环境；密码未设置时程序会无回显提示输入，且生成的 JSON 不包含账户、密码或业务数据：

```bash
$env:EMT_PERF_HOST = '<test-host>'
$env:EMT_PERF_PORT = '<test-port>'
$env:EMT_PERF_ACCOUNT = '<test-account>'
.venv/bin/emt-gateway-perf --driver native-readonly --sdk-root ./EMT_python_SDK/emt_api_python --iterations 3 --output ./artifacts/performance/native-readonly.json
```

若需要三个模拟盘账户，复制 `tools/performance/emt-sim-accounts.example.json` 到 `data/performance/config/emt/emt-sim-accounts.json` 并只在本机填入新密码；`data/` 已被 Git 忽略。必须替换整个占位符，不能保留尖括号。随后以 `--accounts-file ./data/performance/config/emt/emt-sim-accounts.json` 运行。该配置不会写入报告；驱动还会在原生登录期间丢弃 SDK 直接写入控制台的输出，防止登录参数泄漏。

在获得柜台关于速率、并发与测试时段的书面授权前，不得减小 `--interval`、增加并发或将交易及资金类接口加入该 driver。

## 网关完整 C0–C7 只读链路

`gateway-readonly` 是 P3 的低频真实链路驱动。它使用网关自己的 cross-platform loader、`EmtSession`、86 接口 facade、69 SPI bridge、原生 `fastspi`、事件 Journal 和策略 ACK 路径；默认只执行已可完成终结回调的资产、资金、持仓、订单/成交、配股和发行信息查询。每次运行只选一个账户档案，固定至少 250 ms 间隔，最多 10 轮。原生 fastspi 未编译时会拒绝执行。

以下命令会真实登录**普通模拟盘**并执行一次 8 项完整 C0–C7 的只读查询；报告和 SDK 运行文件位于 Git 忽略的 `artifacts/performance/`，不含凭据和业务 payload：

```bash
.venv/bin/emt-gateway-perf --driver gateway-readonly --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --profile ordinary --readonly-iterations 1 --interval 0.25 --output ./artifacts/performance/gateway-readonly-ordinary.json
```

每次真实运行还会自动生成同名的 `.traces.jsonl` 归档；它只包含 C0–C7 元数据、单调时间戳、结果和白名单资源指标。可将多个相同环境、相同 profile 的归档一次性传给通用报告器，精确累计 P0 样本而不重放柜台调用：

```bash
.venv/bin/emt-gateway-perf --driver gateway-trace-report --trace-input ./artifacts/performance/gateway-readonly-ordinary-batch01.traces.jsonl ./artifacts/performance/gateway-readonly-ordinary-batch02.traces.jsonl --output ./artifacts/performance/gateway-readonly-ordinary-aggregate.json
```

只有聚合报告中某场景具备全部所需路径、零失败并达到该场景的 P0 样本门槛时，它才显示为 `passed_p0`；这不等同于生产 SLA 或全矩阵验收。

信用账户执行三个档案均适用的 6 项只读查询。期权账户执行其中 5 项：本轮模拟柜台对 `QueryFund` 已连续 4 次复现“调用返回成功但没有终结 callback”，因此该项只能作为显式诊断探针，不能混入通过的 C0–C7 基线。这样既不会把普通账户专属 IPO/配股场景错误归入信用/期权矩阵，也不会伪造期权资金查询的性能样本。

当前 SDK/模拟柜台组合中，普通账户的 `QueryIPOInfoList`、`QueryYesterdayAsset`，以及期权账户的 `QueryFund`，均已在多轮实测中确认“调用返回成功但未交付终结 callback”。它们仍保留在完整 86 接口矩阵，默认不混入可通过的 C0–C7 基线；需要复核供应商问题时可显式加 `--probe-known-callback-gaps`，其结果会以 `known_no_terminal_callback` 单列，且不会伪装为通过样本。

该驱动仍不能构成全接口验收：变更型接口必须经过单独审查的 fixture 和限速方案。

## 受控全查询 fixture

为其余 `QUERY` 接口生成本机配置模板。模板逐项列出官方矩阵中每个查询与适用账户档案，初始全部 `enabled: false`，因此生成模板本身不会登录或调用 EMT：

```bash
.venv/bin/emt-gateway-perf --driver query-fixture-template --output ./data/performance/fixtures/emt/emt-query-fixture.json
```

仅在本机为已审查的查询填写 `business_args`（不包含由网关注入的 `session_id`、`request_id`），将相应项设为 `enabled: true`。模板已为 `QueryPosition`、`QueryOrders`、`QueryTrades` 写入 SDK 认可的空查询条件；其余参数由受控 fixture 填写。配置校验会拒绝缺失/新增的矩阵项，以及账号、密码、主机、端口、地址、token 和 callback payload；不要试图在该文件中保存这些信息。随后可按账户档案真实执行已启用的**只读**查询：

```bash
.venv/bin/emt-gateway-perf --driver gateway-query-fixture --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --query-fixture ./data/performance/fixtures/emt/emt-query-fixture.json --profile credit --readonly-iterations 1 --interval 0.25 --output ./artifacts/performance/gateway-query-fixture-credit.json
```

它和 `gateway-readonly` 使用同一 Session、facade、SPI bridge、native fastspi、Journal 和 ACK 路径，但只接受官方 `Query*` 调用；下单、撤单、资金、信用还款、行权与组合操作会在配置校验前被排除。参数只保留在被 Git 忽略的本机配置中，不会进入报告；报告只保留接口名、档案、阶段时间戳、结果，以及非零原生返回码（纯数字）的脱敏归因。对每次串行查询，`run.operation_observations` 还会记录终结状态、callback 数和结果对象数；它不写入任何回包、余额、标的、订单/债务 ID 或分页游标，可仅用于判断后续受控 fixture 是否具备对象前提。

若某个本机已启用场景已被确认缺少终结 callback，可重复使用 `--query-case` 仅选择其余已启用场景；该开关不会启用 fixture 中原本禁用的接口，也不能注入或修改业务参数。未知、禁用或重复接口名会在登录前被拒绝。例如：

```bash
.venv/bin/emt-gateway-perf --driver gateway-query-fixture --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --query-fixture ./data/performance/fixtures/emt/emt-query-fixture.json --profile credit --query-case QueryCreditAssetDebtInfo --query-case QueryCreditFundInfo --readonly-iterations 1 --interval 0.25 --output ./artifacts/performance/gateway-query-fixture-credit-selected.json
```

## 变更接口 fixture 准备

以下命令只生成完整、默认 disarmed 的本机模板，不会登录或发送 EMT 请求：

```bash
.venv/bin/emt-gateway-perf --driver mutation-fixture-template --output ./data/performance/fixtures/emt/emt-mutation-fixture.json
```

模板逐项覆盖下单、撤单、资金划拨、信用和期权变更操作。后续实际 driver 只允许在 `armed: true`、每项具有正速率限制、对账查询和唯一客户端 ID 时运行；账号、密码、柜台地址及业务回包不得写入模板或报告。

### 信用还款受限 P0 兼容性

`gateway-credit-repayment` 是一个独立的、受上限约束的模拟盘兼容性 driver。它接受 `CreditCashRepay`、`CreditCashRepaySpecifiedDebt`，以及隔离运行的 `CreditSellStockRepayDebtInterestFee`；前两者必须使用 `QueryCreditCashRepayInfo` 对账、限制每秒最多 1 笔、金额不超过 1。卖券还款只能单笔 100 股、市场对应的非挂单价格类型，且 fixture 必须在私有文件中预先保留 `order_client_id`；它以返回的 `order_emt_id` 使用 `QueryOrderByEMTID` 对账。全部路径均须显式传入 `--execute-simulated-mutations`，并在 native 调用前持久化 hash-only `COMMAND_PREPARED` / `CALL_STARTED`。

信用还款没有客户端幂等 ID，driver 因此绝不自动重试。收到非零原生 ID 但未收到 callback 时，仍会执行一次只读对账，然后停止并将结果保留为 `submit-unknown`；不得重复执行命令来取得样本。卖券还款的预留 `order_client_id` 仅用于进程丢失后的只读归属恢复，绝不写入报告、trace 或 Journal payload；同步 `0` 是确定本地拒绝，报告仅记录 `GetApiLastError` 的整数错误码。仅 `CreditCashRepay` 可用 `--credit-repayment-cycles` 做最多 30 次的串行、1 单位 P0 取样；它是受控兼容性基线，**不是**并发或高吞吐压测。`CreditCashRepaySpecifiedDebt` 与卖券还款一律单次运行，并在异常或对账不一致后停止。

```bash
.venv/bin/emt-gateway-perf --driver gateway-credit-repayment --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --mutation-fixture ./data/performance/fixtures/emt/emt-mutation-fixture.json --credit-repayment-cycles 30 --callback-timeout 8 --interval 1 --execute-simulated-mutations --output ./artifacts/performance/gateway-credit-repayment.json
```

### 期权最小建仓与撤单

`gateway-mutation --profile option` 复用 durable `InsertOrder` / `CancelOrder` 闭环，但只接受已由 `QueryOptionAuctionInfo` 发现的 8 位期权合约、`business_type=10`、开仓（`side=1` 或 `2`、`position_effect=1`）、数量 1、限价和 `ep_id=0`。它每秒最多一条命令、每批最多 10 个下单/撤单周期、没有自动重试；每个命令均按返回 EMT ID 用 `QueryOrderByEMTID` 对账。该通道用于在模拟盘建立最小期权持仓，以及测量通用订单/撤单链路，不能替代组合申报或行权的独立 driver。

```bash
.venv/bin/emt-gateway-perf --driver gateway-mutation --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --mutation-fixture ./data/performance/fixtures/emt/emt-mutation-fixture.json --profile option --mutation-cycles 1 --callback-timeout 8 --interval 1 --execute-simulated-mutations --output ./artifacts/performance/gateway-option-provision.json
```

## 供应商全接口模拟盘扫描

`vendor-simulated-sweep` 将供应商随 SDK 提供的 `test/tradertest.py` 在内存中注入本机模拟盘配置后运行；其原始请求参数、调用顺序和交易/资金/期权路径不被修改，只有账号、地址、日志等级、等待时间和观测包装被替换。为补足供应商样例遗漏的已绑定接口，扫描额外调用心跳、错误/订单归属查询、期权组合持仓和担保卖券还息。它会执行有状态接口，仅能用于已经获授权的模拟盘。

扫描会丢弃供应商输出和业务数据，只报告各绑定方法的调用返回延迟与回调数量；它不能替代带 request-id 关联的 C0→C5 验收基线。每个账户单独运行一次：

```bash
.venv/bin/emt-gateway-perf --driver vendor-simulated-sweep --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --profile ordinary --output ./artifacts/performance/vendor-ordinary.json
```
