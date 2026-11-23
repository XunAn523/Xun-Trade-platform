# HyperPerf 第一版 HTTP 脚本联通验收

## 1. 范围与文件

上传入口为 `hyperperf_http_smoke.py`；文件自包含，不需要上传 TradeCore 包或本目录测试。
`operation_names.json` 是场景名称清单，不是 HyperPerf 计划导入文件。

本场景只验证：脚本上传 → Locust → 平台受控代理/白名单 → HTTP 隔离靶 → 业务断言 →
请求统计、错误及原始产物。它**不访问 TradeCore HTTP/UDS、Journal、EMT 或 QMT**，
通过也不代表这些链路、生产 SLA 或 2,000 TPS 已通过。

合同来源为 HyperPerf Git `3e442a9` 的 `test-targets/http/server.py`、
`test-targets/http/load_test.py` 和 `extensions/locust-runner`。没有从示例推测交易接口。

## 2. 请求及业务成功条件

```http
GET /work HTTP/1.1
Accept: application/json
```

无 query、请求体、账户、密码、token 或额外鉴权 Header。成功响应：

```http
HTTP/1.0 200 OK
Content-Type: application/json
Connection: close

{"accepted":true}
```

靶服务的合同允许 HTTP 客户端发起 HTTP/1.1 请求；当前 Python fixture 响应为 HTTP/1.0，
并显式关闭连接。脚本不要求响应 HTTP 版本，不额外启用连接复用或重试。

每次 task 只发送一次 `GET /work`。全部满足以下条件才调用 Locust `response.success()`：

1. 没有连接/传输错误；
2. HTTP 状态码严格为 `200`，不自动跟随 3xx；
3. Content-Type 的 media type 为 `application/json`，允许 charset 等参数；
4. JSON 是且仅是 `{"accepted": true}`，不接受 `1`、字符串 `"true"`、缺字段或额外字段。

失败使用 `catch_response=True` 正常计入同一请求，不重复发送 request event：

| 情况 | 处理 |
| --- | --- |
| 连接失败、连接/读取超时 | 将原始 `response.error` 交给 Locust，保留异常类型；按平台实际分类记录 `TIMEOUT` 或 `SYSTEM_ERROR`。某些超时被底层封装为连接异常时可能归为 `SYSTEM_ERROR`，应结合原始失败信息核对 |
| 非 200（包括 409、429、503） | `HTTP_STATUS expected=200 actual=...`；不能作为成功 |
| Content-Type 不符或 JSON 解析失败 | `PROTOCOL_ERROR ...` |
| `accepted` 不严格为 true 或对象结构不符 | `BUSINESS_CONTRACT ...` |
| 非预期脚本异常 | 不静默捕获；检查 Locust exceptions 与平台执行状态 |

`/work` 没有定义正常业务拒绝响应。因此不能把任意 409 或异常响应擅自标注为已知
`BUSINESS_REJECTION`。本脚本不访问 `/reject`、`/error`，不做故障演练。
错误文本不输出响应体。连接异常可能包含本机测试 origin，不能用于承载秘密。

连接超时 `2 s`、读取超时 `5 s` 是单次请求 I/O 预算，不是压测时长或严格整次调用 deadline。
请求等待平台发起槽位的时间不等同于 Locust HTTP response time；业务 JSON 断言发生在取得
响应后，不能宣称它的 CPU 开销已包含在 HTTP 延时中。

## 3. HyperPerf 填写内容

| 项目 | 当前确认值 |
| --- | --- |
| 环境名称 | `FE-UAT-20261013-01-env` |
| 环境 ID / 版本 | `79bb0692-c75e-4807-947f-b27504857d59` / `8` |
| 场景类型 / `scenario.kind` | `HTTP` |
| Runner | `io.hyperperf.locust@1.0.0` |
| 入口文件 | `hyperperf_http_smoke.py` |
| host | `http://127.0.0.1:18981` |
| `scenario.operation_names` | `["GET /work"]`，大小写及空格逐字一致 |
| 目标白名单 | `127.0.0.1:18981` |
| 负载模型 / `load.model` | `CONSTANT_RATE` |
| secret refs | 不需要；不注入任何秘密 |
| 外部依赖 | 平台已有 `locust==2.31.5`，脚本没有额外依赖 |
| TLS / 私有 CA / 客户端证书 | 当前 HTTP 靶不涉及 |

host 必须来自该 HyperPerf 环境的冻结配置，不能包含 `/work`、账号密码、query 或 fragment。
不要修改脚本填 host，不要设置代理环境变量、`Proxy-Authorization` 或 `proxies=`，也不要
关闭平台隔离。脚本不配置 `host`、users、spawn rate、wait time、目标速率或运行时长。

平台构建预检必须确认实际加载 Locust **2.31.5**；不要在脚本中 pip install、下载文件或
自动启动 Locust。TradeCore 服务需要 CPython 3.10，不代表本脚本要求 HyperPerf Runner
使用 3.10；使用平台已锁定并验证的运行时即可。

## 4. 独立场景计划（在平台填写，不是可直接导入的 API JSON）

本脚本是新脚本资产。创建、发布或执行计划前，须由负责人根据当时的隔离目标、
脚本版本、共享服务器状态、负载与执行时段明确放行。此前 FE-20～FE-23 的放行
只针对其指定的旧脚本与计划版本，不自动适用于本脚本。

首轮可向负责人申请低负载参数：`CONSTANT_RATE`、`users=1`、`spawn_rate=1`、
`target_rate=1` 请求/秒、`max_in_flight=2`、预热 `0` 秒、测量 `10` 秒、
停止宽限 `5` 秒。以上为申请参考，不是对本脚本的运行授权；指标窗口按平台的
有效配置确定。只有获得本脚本的明确放行并通过预检后，才能执行。

`users`、`spawn_rate` 对应 Runner 配置，`target_rate` 属于平台 `load`，各时长属于平台
`phases`，窗口属于 `collection`。以平台实际预检可填字段为准，不手工修改冻结快照。
users 是并发上限，不表示这些连接始终同时在途。target_rate 是发起上限，不保证完成 QPS；
固定速率调度可能在首尾边界产生差异，不把期望速率 × 秒数硬充为实际完成数。

本脚本不自动启动或升压。更高并发、速率、长稳和容量测试均待负责人单独确定目标、
上限与执行窗口；既有业务指标或需求目标不等于当前测试授权。

## 5. 数据、准备与清理

- 平台管理员确认该环境仍指向隔离 fixture，且 `/work` 合同与上述已审查代码一致。
- 先完成环境版本、目标白名单、依赖、资源和平台预检；不能以 HTTP `/health` 加到负载 task
  中替代预检。本脚本只有 `GET /work` 一个 operation。
- 无业务测试数据需要准备。每个成功处理的 `/work` 仅增加靶内存 `work` 计数。
- 不 reset 服务、不删除目录、不停止 TradeCore，不在客户端创建替代靶。
- 靶计数可跨运行累积；若用 `/stats` 人工核对，记录本轮前后差值并排除其他访问，不能把
  累积计数当本轮吞吐。不要额外访问 `/work` 做人工健康检查后又计入本轮差值。
- `/stats` 不是脚本的负载 operation；如需通过 HyperPerf 读取，应单独配置只读核对场景。

## 6. 校验与通过标准

### 本地静态/逻辑验证

从 TradeCore Python 根目录，在**已有**开发环境运行：

```powershell
.\.venv\Scripts\python.exe -m py_compile tools\hyperperf\http_smoke\hyperperf_http_smoke.py
.\.venv\Scripts\python.exe -m pytest -q tests\test_hyperperf_http_smoke.py
.\.venv\Scripts\python.exe -m ruff check tools\hyperperf\http_smoke tests\test_hyperperf_http_smoke.py
```

单元测试用隔离的 Locust API 替身检查成功/失败断言、异常保留、每 task 一次请求、稳定
operation 及无代理/事件覆盖；它不证明实际 Locust 安装、网络连通或 HyperPerf 端到端通过。
没有 Locust 的本地环境无需为了这些单元测试安装它。

### 实际依赖和平台冒烟

待共享项目维护结束、负责人明确放行本脚本的计划后，由平台锁定运行时的依赖预检
确认 `locust==2.31.5`，再在隔离目标执行获准的冒烟计划。
必须有真实请求结果、非空统计和原始产物；零请求/构建失败/脚本加载失败不能算通过。

逐项核对：

1. 只有预期 `GET /work` 被测名称（汇总 Total 不算额外业务操作）；测量阶段成功数大于零。
2. 测量阶段 HTTP/合同/连接/超时失败均为零，exceptions 无未解释错误，平台未报告数据缺口。
3. 按同一阶段、同一窗口核对计数。跨窗在途请求不得强行配平；测试结束并排空后再检查
   started/terminal/in-flight，预热请求不能混入 measurement 的 QPS。
4. 保存平台原生 stats/failures/exceptions/history CSV、JSONL/HDR_V2 窗口和执行摘要
   （以实际部署产物清单为准），不手工生成、编辑或补零。
5. target_rate 与实际 offered/completed 分别报告；若资源不足未达到目标，如实记录。
6. 延时按平台实际可用结果报告，并保留原始 HDR/CSV。当前正式报告可能标记
   `HDR_DISTRIBUTION_NOT_MERGED` 且分位数为空；只有存在可信计算结果时才报告
   P50/P95/P99/P100，不得从空值推算或伪造。微秒单位不代表测量误差一定达到微秒级。
7. 用户终止、强杀、停止宽限超限及未收敛在途请求必须保留在结果中，不能改写为通过。

当前本机生成/单元测试不等于服务器冒烟；正式上传执行的结果应单独归档。

## 7. 当前未覆盖

- TradeCore OMS/EMS HTTP 性能入口及其 fixture/分片初始化；下一份脚本另行接入。
- Consumer 原生 UDS、SO_PEERCRED 授权、Journal durable boundary、生产 checkpoint。
- 真实 EMT/QMT 委托、回报、清理或生产能力放行。
- WebSocket、HTTP/2 mTLS 控制面、JWE 或任何凭据传递。
- 故障演练、长运行、饱和容量和 2,000 并发同时释放。

不需要新增平台接口或传递秘密即可运行当前 HTTP fixture 场景。UDS 若后续纳入，必须
先实现/批准受控 Runner 能力；不能在此脚本直接访问宿主机 socket 或绕过代理。
