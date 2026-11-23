# EMT Gateway 压测工具

本目录是 Linux x86_64、catalog 驱动的压测骨架。它覆盖目标 EMT API catalog 的 86 个主动方法与 69 个回调，并拒绝产生缺项报告。它**不是**真实柜台模拟器：默认 `synthetic` driver 只度量 Python/gateway 测试链路开销，不能用来声明 EMT 柜台延迟或吞吐。Windows 不属于压测、验收或发布范围。

## 目录

```text
tools/performance/
  pyproject.toml                    # 可安装的独立压测包
  perfbench/
    catalog.py                      # 86/69 名称、档位、workload 的唯一测试清单
    matrix.py                       # 全接口 × 账户档案 × workload × C0-C7 路径矩阵
    driver.py                       # 可注入 driver 契约与 synthetic 实现
    trace.py                        # 无业务载荷的关联 ID、C0-C7 与资源指标采集器
    gateway_report.py               # 端到端 trace 的严格报告汇总器
    workload_plan.py                # 非稳态 fixture 的控制项与执行前校验
    histogram.py                    # 精确 percentile 汇总
    runner.py                       # 覆盖执行、报告与完整性校验
    cli.py                           # `emt-gateway-perf` 命令
  tests/
    test_catalog.py                 # 86/69、唯一性与档位覆盖
    test_runner.py                  # 报告完整性与缺项拒绝
```

## 本地验证

在项目的 CPython 3.10 `.venv` 中执行：

```bash
.venv/bin/python -m pip install -e ./tools/performance
.venv/bin/python -m unittest discover -s ./tools/performance/tests -v
.venv/bin/emt-gateway-perf --driver synthetic --iterations 100 --output ./artifacts/performance/synthetic.json
```

所有命令均面向 Linux；`artifacts/performance/` 是本机结果目录，不能提交为已批准基线。

## 接入真实网关

先生成不可提交、无账号信息的执行清单：

```bash
.venv/bin/emt-gateway-perf --driver runbook-template --output ./data/performance/plans/gateway/gateway-perf-runbook.json
```

真实网关在一个 `GatewayTraceCollector` 实例上记录同一个 `correlation_id` 的 `C0` 至 `C7`。主动调用从 `C0` 开始，在准入、持久化、binding 前后分别记录 `C1` 至 `C4`；关联的原生 callback 继续记录 `C5` 至 `C7`。callback 自身从原生入口开始记录 `C0`，并在最小副本进入 ring 时记录 `C5`。同时只允许记录 `callback_ring_depth`、`worker_queue_depth`、`journal_queue_depth`、CPU、RSS、GC、丢弃、流控和 native 错误等白名单指标。

trace 为一行一个 JSON 对象；它没有订单、账户、密码、地址、标的或 callback 业务字段。完成测试后生成报告：

```bash
.venv/bin/emt-gateway-perf --driver gateway-trace-report --runbook ./data/performance/plans/gateway/gateway-perf-runbook.json --trace-input ./artifacts/performance/gateway-traces.jsonl --output ./artifacts/performance/gateway-c0-c7-report.json
```

报告会为每个 catalog 场景给出 `pending`、`incomplete`、`insufficient_samples`、`failed` 或 `passed_p0`。缺失接口、缺失 workload、缺失 C0-C7 路径和失败样本都不能被汇总为通过；`passed_p0` 也不是生产 SLA，仍需对照批准的基线完成 release 样本量和预算判断。

若经复核确认某一归档文件是**本地压测器缺陷**而非柜台结果，汇总时仍须把该文件作为输入显式列出，再用 `--exclude-trace-input` 和非空 `--trace-exclusion-reason` 排除。报告会固化输入、纳入、排除批次数、文件名和理由；不能排除未传入的文件、不能无理由排除，也不能排空全部输入。这样既保留真实柜台失败，也能防止已修复的本地关联/对账缺陷持续污染全局门禁。

归档数量达到 shell 参数长度限制时，使用可重复的 `--trace-input-directory` 递归收集 `*.traces.jsonl`；它会去重，并将目录写入同一来源记录。受控本地注入目录仍必须显式排除，不能混入真实 EMT 证据。

```bash
.venv/bin/emt-gateway-perf --driver gateway-release-readiness --trace-input ./artifacts/performance/valid.traces.jsonl ./artifacts/performance/invalid-local-driver.traces.jsonl --exclude-trace-input ./artifacts/performance/invalid-local-driver.traces.jsonl --trace-exclusion-reason "reviewed local harness defect; not counter-performance evidence" --output ./artifacts/performance/release-readiness.json
```

## 发布预检与强制门禁

`gateway-release-readiness` 将同一份完整 trace 证据依次检查 P0 覆盖、已批准的全矩阵基线与严格验收预算。无论当前是否通过，它都会生成 payload-free 阻断清单，便于 CI 和发布评审看到缺口；没有批准基线、任一场景未达 P0、样本不足、失败或 p99 超预算都会保持 `release_ready: false`。

```bash
.venv/bin/emt-gateway-perf --driver gateway-release-readiness --trace-input ./artifacts/performance/gateway-traces.jsonl --baseline ./data/performance/plans/gateway/performance-baseline.json --output ./artifacts/performance/release-readiness.json
```

在 CI 的发布阶段增加 `--require-release-ready`。该开关会先写出报告，再以退出码 `2` 阻断流水线；它不会把供应商阻塞、缺少 callback 或未批准预算视为通过。

```bash
.venv/bin/emt-gateway-perf --driver gateway-release-readiness --trace-input ./artifacts/performance/gateway-traces.jsonl --baseline ./data/performance/plans/gateway/performance-baseline.json --require-release-ready --output ./artifacts/performance/release-readiness.json
```

真实 driver 不得内置账户、密码、Token、券商地址或跳过 capability gate。若运行时 86/69 差异不为零，报告必须将相应接口列为 `unsupported` 并拒绝“全覆盖”验收。

## 同会话原生 SDK 与网关 A/B 对比

`gateway-ab-order` 用于定位“网关本地附加延时”和 EMT/网络/柜台等待的相对位置。它在**同一台主机、同一模拟账户、同一个活跃 EMT 会话**中，以低频串行方式交替执行两条受控的 `InsertOrder → CancelOrder` 路径；奇数周期先执行原生直调，偶数周期先执行网关路径，降低固定顺序造成的偏差。

- `native_direct`：直接调用已加载的 EMT Python binding，测量 `C0→C4`、`C4→C5`、`C0→C5`。该路径没有命令日志、事件日志或 ACK，故 `C0→C2`、`C2→C4`、`C5→C6`、`C6→C7` 明确标记为 `not_applicable`，不会以零值代替。
- `gateway_full`：通过 `OrderGateway`、严格 `FileJournal`、`EmtOrderAdapter`、原生 fastspi、事件日志和 ACK 运行，分别输出 `C0→C2`、`C2→C4`、`C4→C5`、`C5→C6`、`C6→C7` 的 `p99_ns` 和 `p999_ns`。
- `observed_ab_delta`：按相同周期、相同接口比较 `gateway_full - native_direct` 的 `C0→C4`、`C0→C5`。它是顺序 A/B 观察值，不能将差值直接归因于某一 EMT 网络或柜台组件。

默认仅校验已武装的本机 mutation fixture，不加载 SDK、不登录也不发单：

```bash
.venv/bin/emt-gateway-perf --driver gateway-ab-order --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --mutation-fixture ./data/performance/fixtures/emt/emt-mutation-fixture.json --profile ordinary --ab-cycles 10 --interval 1 --output ./artifacts/performance/gateway-ab-order-preflight.json
```

只有在模拟盘 fixture 已完整配置且 `armed=true`，并且明确传入 `--execute-simulated-mutations` 时才实际执行：

```bash
.venv/bin/emt-gateway-perf --driver gateway-ab-order --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --mutation-fixture ./data/performance/fixtures/emt/emt-mutation-fixture.json --profile ordinary --ab-cycles 10 --interval 1 --execute-simulated-mutations --output ./artifacts/performance/gateway-ab-order.json
```

每个周期至多发送四笔原生指令（原生直调下单/撤单各一笔，网关下单/撤单各一笔），命令间隔不得低于一秒，且全程禁止自动重试。原生直调分支故意不写网关命令日志，仅用于比较；任一原生直调失败或回调超时后，必须先按券商权威状态查询完成**人工核对**，不得直接重跑。

## 非稳态场景的受控执行清单

已有的低频只读与订单归档只覆盖 `steady` 工作负载，不能证明突发、空结果、分页、迟到回调、故障、重连或恢复的性能。使用下列命令生成本机、不可提交的非稳态控制清单：

```bash
.venv/bin/emt-gateway-perf --driver workload-plan-template --output ./data/performance/plans/gateway/gateway-workload-plan.json
```

已确定负责人时，可在生成时一次性写入其不含敏感信息的角色/姓名；这不会填充或批准 fixture：

```bash
.venv/bin/emt-gateway-perf --driver runbook-template --fixture-owner fengyu --output ./data/performance/plans/gateway/gateway-perf-runbook.json
.venv/bin/emt-gateway-perf --driver workload-plan-template --fixture-owner fengyu --output ./data/performance/plans/gateway/gateway-workload-plan.json
```

该清单覆盖矩阵中全部非 `steady` 场景，并为每个 workload 固定所需控制项。例如突发必须有已批准 lane、持续窗口和背压观测；分页必须有多页 fixture 与尾页核对；恢复/重连必须有故障注入及恢复或对账核对。只允许填入本地受控配置的 `fixture_id`、负责人、控制登记引用和恢复核对引用，不能写入账号、端点、标的、订单参数或故障注入细节。

对外部模拟盘 fixture 执行，除了全矩阵 `runbook` 外还必须传入已填妥且与 runbook 的 fixture/负责人一致的清单；dry-run 可用于提前发现不一致：

```bash
.venv/bin/emt-gateway-perf --driver gateway-fixture --runbook ./data/performance/plans/gateway/gateway-perf-runbook.json --workload-plan ./data/performance/plans/gateway/gateway-workload-plan.json --output ./artifacts/performance/gateway-fixture-preflight.json
```

只有显式加上 `--execute-simulated --fixture-command ...` 才会启动外部进程；此时缺少或未完成的非稳态清单会拒绝执行。该门禁不生成、也不伪造 EMT 测试结果。

### 真实只读查询 burst

`gateway-query-burst` 是首个项目内置的非稳态 fixture：它只使用已启用的本机 `Query*` 参数。每个接口先执行一轮 2 并发、最长 2 秒的 capability probe；只有两个 request-id 都得到完整 callback 链路的接口，才会以 2--4 个独立 request-id 进入扩容。默认 25 轮 × 4 个请求可为每个已选接口产生 100 条 burst 样本；probe 失败的接口只保留真实阻断证据，不会被长时间超时或其他接口的成功掩盖。该 runner 不下单、撤单或变更资金，也不能替代空结果、分页、迟到、恢复或 callback 注入的独立证据。

`gateway-readonly`、`gateway-query-fixture`、`gateway-query-burst` 与 `gateway-query-late` 默认使用 4,096 容量的 native callback ring；可显式设置 `--callback-capacity`（1--65,536）进行受控容量验证。报告会记录实际容量，因而历史容量不足的溢出证据与当前配置验证不会被混为同一基线。

```bash
.venv/bin/emt-gateway-perf --driver gateway-query-burst --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --query-fixture ./data/performance/fixtures/emt/emt-query-fixture.json --profile ordinary --burst-rounds 25 --burst-width 4 --interval 0.25 --output ./artifacts/performance/gateway-query-burst-ordinary.json
```

### 真实请求的 callback 消费延迟注入

`gateway-query-late` 发起真实只读查询，但在该查询的真实 callback 已进入 fast ring（`C5`）后，才暂停网关消费者 10--1000 ms，再继续 durable/ACK。它测量的是明确标注的**网关消费侧**迟到，不会把该结果描述为柜台响应慢或原生 callback 晚到。每个接口的 P0 仍需要累计 100 条完整、零失败 trace。

```bash
.venv/bin/emt-gateway-perf --driver gateway-query-late --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --query-fixture ./data/performance/fixtures/emt/emt-query-fixture.json --profile ordinary --query-case QueryETF --late-iterations 10 --late-delay-ms 100 --interval 0.25 --output ./artifacts/performance/gateway-query-late-ordinary.json
```

### 查询消费者的空结果与分页语义

`query-semantic-fixture-template` 生成独立、被忽略的本机配置。它默认启用八项与当前网关消费者直接相关的、无副作用的检查：三类账户档案的 `QueryOrders` / `QueryTrades` 未来时间窗空结果，以及普通账户 `QueryETFByPage` / `QuerySecurityByPage` 的一页一条、回调游标续查第二页。其余账户状态、权限或业务标的依赖的项均默认禁用。

```bash
.venv/bin/emt-gateway-perf --driver query-semantic-fixture-template --fixture-owner fengyu --output ./data/performance/fixtures/emt/emt-query-semantics.json
```

`gateway-query-semantics` 只保留终态、非空数据对象计数和用于下一页的整数 `reference`；不保留任何回包字段。空结果要求终态 callback 且计数为零；分页要求首尾两页各有数据对象、首页回传可续查游标且第二页完整结束。语义不满足时，对应 trace 不会进入 P0 证据。

```bash
.venv/bin/emt-gateway-perf --driver gateway-query-semantics --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --semantic-fixture ./data/performance/fixtures/emt/emt-query-semantics.json --profile ordinary --semantic-workload empty --semantic-iterations 100 --interval 0.25 --output ./artifacts/performance/gateway-query-empty-ordinary.json

.venv/bin/emt-gateway-perf --driver gateway-query-semantics --sdk-root ./EMT_python_SDK/emt_api_python --accounts-file ./data/performance/config/emt/emt-sim-accounts.json --semantic-fixture ./data/performance/fixtures/emt/emt-query-semantics.json --profile ordinary --semantic-workload pagination --semantic-iterations 50 --interval 0.25 --output ./artifacts/performance/gateway-query-pagination-ordinary.json
```

在执行前或归档 trace 后，可生成单独的非稳态就绪报告。它把清单完整性、主 runbook 的 owner/fixture 一致性和各场景真实 C0--C7 P0 证据分开列出；任一项缺失都会保持 `nonsteady_ready: false`：

```bash
.venv/bin/emt-gateway-perf --driver gateway-workload-readiness --workload-plan ./data/performance/plans/gateway/gateway-workload-plan.json --trace-input ./artifacts/performance/gateway-traces.jsonl --output ./artifacts/performance/gateway-workload-readiness.json
```
