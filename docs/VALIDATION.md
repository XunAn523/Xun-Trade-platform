# XunTradeOMSEMS 验证记录

验证日期：2026-11-23（Asia/Shanghai）。本次只执行离线与本地测试，没有连接券商或提交实盘订单。

本文保留各阶段的执行范围；最新结果见末尾的“2026-11-26：六项优化交付”。

## 已完成检查

| 检查 | Windows x64 | Ubuntu / WSL2 x64 |
| --- | --- | --- |
| CPython | 3.10.11 | 3.10.18 |
| C++ 编译器 | MSVC 19.44 | GCC 13.3 |
| pip 安装原生扩展 | 通过 | 通过 |
| CMake Release / CTest | 1/1 通过 | 1/1 通过 |
| native 离线自检 | 通过 | 通过 |
| 原生后端核心/兼容回归 | 108 通过，16 跳过 | 190 通过，12 跳过 |
| Python 诊断后端回归 | 53 通过 | 53 通过 |
| C++ AddressSanitizer + UndefinedBehaviorSanitizer | 未执行 | 通过 |
| wheel 构建、包内容和隔离导入自检 | 通过 | 未执行 wheel 隔离检查 |

原生回归覆盖新增计算、现有账户路由、journal、gateway、scheduler、OMS、会话执行权、恢复与
订单账本。Linux 额外覆盖 OMS/EMS sidecar、mmap journal、committed recovery、投影存储、
Consumer 读模型与 FastSPI v2 runtime。跳过项遵循原测试中的平台/环境条件，未作为通过计数。

新增计算测试覆盖：

- CRC32C 标准向量、空输入、跨长度/非对齐数据和旧 journal 接口的实际绑定；
- BLAKE2s-128 与 `hashlib` 的随机、Unicode、64 字节边界及多块结果一致性；
- 串行、并行、多个 Python 线程同时调用时输出一致；
- int64 上界、乘法溢出、负数、现金与订单限额优先级；
- 10,000 条随机金额输入与 Python 任意精度整数计算的一致性；
- bool/float/越界整数、超批次、非法 worker/分片数的拒绝；
- 原生扩展缺失时强制 native 失败，以及显式 python/auto 诊断模式；
- 原生运行错误传播，不静默切回 Python；
- EMS 批量路由与单笔路由一致，跨账户和非法身份被拒绝。

改动文件的 Ruff lint/format 检查、`uv lock --check` 和 Windows `pip check` 均通过。
发行名/CLI 策略的独立测试为 8 项通过（历史全仓库边界断言单独比较，见下文）。
Windows wheel 验证包含 `.pyd`、原生类型声明、Consumer schemas 和 SQL migration，
并从独立安装目录导入后通过完整离线自检。

## 性能实测

同一台 20 逻辑 CPU 的主机，7 次采样中位数；1KB CRC 每样本调用 1,000 次，
分片和金额计算每批 50,000 行。时间包含 Python 调用、输入转换和输出创建。

| 项目 | Windows Python | Windows C++ | Windows 倍率 | Linux Python | Linux C++ | Linux 倍率 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| CRC32C，1,000 × 1KB | 60.06 ms | 0.147 ms（1 worker） | 408.28× | 54.88 ms | 0.151 ms（1 worker） | 364.35× |
| 批量 session 分片 | 21.38 ms | 6.43 ms（4 workers） | 3.32× | 23.39 ms | 3.84 ms（4 workers） | 6.10× |
| 独立金额计算 | 34.44 ms | 4.52 ms（1 worker） | 7.62× | 30.73 ms | 3.33 ms（4 workers） | 9.24× |

原始记录：[Windows](benchmarks/windows-x64.json)、[Linux](benchmarks/linux-x64.json)。
每个报告同时保留 C++ 单线程和 4 线程数据；Windows 的金额计算单线程更快，说明轻量计算
应优先使用 `workers=1`，不要假设增加线程一定更快。

运行方法：

```bash
python -m tools.benchmarks.hybrid_compute --batch-size 50000 --repeats 7 --workers 4 --output benchmark-results.local.json
```

这是计算层对照实验，不包含 fsync、网络、账户授权、OMS 领域校验或券商服务。
结果不能外推为真实交易吞吐或 C0→C7 延迟保证；WSL2 结果也不能替代生产 Linux 主机验收。

## 来源代码的既有检查与限制

原始代码导入时已存在全仓库检查问题，本次没有宣称历史测试/检查全部通过：

- 原包边界检查有 **9 项既有导入边界问题**。对源目录和新目录的检查结果按路径/规则比较一致；
  新发行名与 CLI 元数据检查为 0 项问题。
- 原 type-ignore 检查有历史违规，涉及导入测试和工具；本次新增代码没有引入 type-ignore。
- 原敏感内容扫描有 5 处误报，人工检查为测试占位密码和变量/函数引用。
  另行扫描全部导入文件，未发现私钥、GitHub token 或真实账户凭据；未导入本地 secrets/SDK/data。
- 全仓库 pytest 收集需要原有可选工具（如 Schemathesis、hdrhistogram、uvicorn）和部分历史文件；
  初次广泛收集遇到这些缺失。核心与新计算回归使用上表范围，未把收集失败算作通过。
- SDK、PostgreSQL、远端部署、历史 artifacts 和实盘环境未纳入本轮验收。

GitHub Actions 配置了两个平台的构建、自检、核心回归及后端对照。配置已提交，
本报告不把尚未观察到结果的远端 Actions 运行计为通过。

## 2026-11-26：C++20 构建标准复核

所有项目 C++ 目标统一使用 ISO C++20，包括核心库、CPython 绑定和原生测试。
CMake 固定标准为 20，禁止降级和 GNU 扩展；设置为 17 或 23 时配置失败。
公共原生头文件拒绝低于 C++20 的直接编译，原生测试在编译期断言 `__cplusplus == 202002L`。
MSVC 的 CMake 与 pip 构建均启用 `/Zc:__cplusplus`。

| 本次检查 | Windows x64 | Ubuntu / WSL2 x64 |
| --- | --- | --- |
| CMake 核心库、Python 绑定、原生测试构建 | 通过 | 通过 |
| CTest | 1/1 通过 | 1/1 通过 |
| pip 原生扩展重建与离线自检 | 通过 | 通过 |
| 构建变更相关核心回归 | 108 通过，16 跳过 | 112 通过，12 跳过 |

逐项检查 MSVC 工程和 GCC 编译命令，三个 C++ 编译单元均采用 C++20。
Windows 的 CMake 扩展和两平台的 pip 扩展实际报告 `cxx_standard: 202002`。
CMake C++17/C++23 配置拒绝检查、直接 C++17 编译拒绝检查均通过。
Windows Python 诊断后端回归为 53 项通过，改动 Python 文件的 Ruff lint/format 检查通过。

本次核心回归范围小于 2026-11-23 的 Linux 扩展回归；两次记录按各自范围解读。
原有 C 回调队列采用独立的 C 构建配置。GitHub Actions 已加入 CMake Python 绑定构建，
本记录只报告本地观察到的运行结果。

## 2026-11-26：六项优化交付

按顺序实现并验证以下改动，所有 C++ 部分继续使用 ISO C++20：

| 项目 | 实现与边界 |
| --- | --- |
| 业务批量入口 | EMS 子单计划一次调用 C++ 批量路由，校验整个批次的账户，失败不发布部分计划；未新增直接交易调用 |
| 端到端测量 | 离线批量路由、有界 scheduler、fsync journal、模拟 broker、回调 ACK；报告吞吐、完成/拒绝、P50/P95/P99、排队与日志同步 |
| 内存与并发预算 | 单批保守记账 64 MiB、进程记账 256 MiB、4 个在途调用、合计 32 个请求 worker；原生和诊断后端均拒绝超预算 |
| 质量门禁 | 9 项模块边界违规归零；EMT 专用 Consumer 组合迁至兼容命名空间，全仓库 Ruff 与敏感扫描通过并进入 CI |
| 常驻线程池 | 按需创建并复用至多 31 个后台 C++20 worker，小批次串行；线程数可观测，fork 继承明确拒绝并要求 spawn |
| 归档分段与保留 | 异步归档使用不保留逐条对象的 FileJournal；按记录/字节轮转，生成可重建索引，封存段 CRC/SHA-256 验证，显式保留策略才清理旧段 |

内存额度是输入转换和输出的保守记账，不是 RSS 上限；不包含调用方已持有的输入和常驻线程栈。
归档是非权威副本，段内 LSN 不作为全局执行序号。默认保留所有历史磁盘段，没有对现有生产归档执行清理。

### 最终回归

同一组 22 个核心、归档、协议、EMS 计划和 EMT/QMT 适配测试文件：

| 环境 | 结果 |
| --- | --- |
| Windows x64 / CPython 3.10.11 | 177 通过、34 跳过，另有 10 个 subtest 通过 |
| Ubuntu / WSL2 x64 / CPython 3.10.18 | 207 通过、4 跳过，另有 10 个 subtest 通过 |
| Windows Python 诊断后端相关回归 | 73 通过、2 跳过 |
| 归档补充回归（含已索引尾段丢失拒绝） | Windows 和 Linux 各 12 项通过 |
| 敏感扫描强化与测试夹具回归 | 33 通过、6 跳过 |
| CMake / CTest，两个平台 | 均 1/1 通过 |
| GCC AddressSanitizer + UndefinedBehaviorSanitizer | 通过，包含线程池运行与正常退出 |
| 全仓库 Ruff、模块边界、敏感内容、uv lock 和 Git whitespace 检查 | 通过 |

新增测试包括：业务调用批量路由、错误不发布计划、内存拒绝、预算归还、线程复用、并发计算一致性、
fork 子进程拒绝而不挂起、归档分段/保留、关闭排空、关闭超时保留所有权、线程启动失败释放所有权、
进程异常退出后的索引重建、封存数据损坏不修复、合法 CRC 内容变化的 SHA-256 检出、已索引尾段丢失拒绝。

泛化敏感扫描保留了 JSON 引号键和未加引号的环境变量凭据检测；测试中的人工凭据统一明确标记为 synthetic。
QMT 引擎为运行时注入的函数添加静态声明，并同步更新探针加载器的引擎 SHA-256；对应锁定哈希回归通过。

### 离线实测与解读

本机 200 请求、非匀速突发、3 个调度 lane、批次 32 的一次本地观察：

| 配置 | 完成 / 拒绝 | 完成吞吐 | 延迟 P95 | 延迟 P99 |
| --- | ---: | ---: | ---: | ---: |
| 每 lane 容量 256，模拟 broker 无延时 | 200 / 0 | 131.10/s | 1457.26 ms | 1503.29 ms |
| 每 lane 容量 4，模拟 broker 延时 1000 us | 13 / 187 | 39.51/s | 327.79 ms | 327.79 ms |

两组均核对 `offered == completed + rejected`、模拟 broker 调用次数等于完成数量，拒绝项没有自动重发。
第一组排队 P99 为 1501.00 ms，而 gateway C0→C7 P99 为 65.93 ms：本次突发长尾主要体现队列积压，
不能用 CRC 或计算微基准的倍率解释完整链路收益。结果包含当前主机与存储状态，不是生产 SLA。

原始记录：[正常突发](benchmarks/offline-pipeline-windows.json)、
[背压](benchmarks/offline-pipeline-overload-windows.json)、
[线程池计算对照](benchmarks/pooled-compute-windows.json)。
线程池对照包含 50,000 行、7 次采样和 Python 转换成本；不宣称已观察到固定的线程池加速倍率。

### 保留的验收边界

全量历史测试仍未宣称通过：一次广泛运行遇到缺失历史证据文件，部分环境测试停滞后终止。
没有通过增加全局忽略或改写历史验收证据来掩盖这些限制。CI 的自动化核心范围在 workflow 中明确列出，
全仓库静态门禁扫描所有受版本控制的业务、测试和工具 Python 文件。
历史 type-ignore 规范整改、外部数据库、真实 SDK、现场执行权和实盘链路不属于以上通过计数。
远端 Actions 运行结果未纳入本地结果；本轮没有发起真实交易。

## 2026-11-26：P0 可复现验收基线

本节更新前述阶段的验收范围与遗留项状态。CI 和本地统一读取
`tools/quality/test_suites.json`：原生核心 25 个文件，Python 对照 3 个文件，
另有逐文件环境排查清单。`--catalog` 生成全仓库测试文件与清单归属，未纳入核心的文件不会视为通过。

| 本地检查 | Windows x64 / Python 3.10.11 | Ubuntu / WSL2 x64 / Python 3.10.18 |
| --- | --- | --- |
| 统一原生核心清单 | 210 通过、34 跳过 | 240 通过、4 跳过 |
| Python 参考后端 | 54 通过、2 跳过 | 54 通过、2 跳过 |
| 未分类跳过 | 0 | 0 |

Windows 原生的 34 项跳过为 31 项平台条件与 3 项 PostgreSQL 前提；Linux 的 4 项为
3 项 PostgreSQL 与 1 项非 Linux 拒绝路径不适用。Python 对照各有 1 项原生实现不适用、1 项平台条件。
每个节点的原因和补验前提都写入 JSON；未知原因会使 runner 失败。
未提供 `EMT_GATEWAY_TEST_POSTGRES_DSN`，数据库三项仍需隔离测试数据库补验。

验收依赖由 `uv.lock` 和 `ci` extra 锁定，uv 固定为 0.13.9、构建后端 setuptools 固定为 84.2.1。
runner 输出提交、工作区状态、依赖锁和清单 SHA-256、逐节点结果、JUnit 和完整日志。
超时保留诊断并终止本次启动的进程树，结果标记为失败；每次运行使用独立目录，避免读取上一次成功结果。
CI 上传验收日志、报告和 wheel，保留 14 天。

隔离打包检查发现增量 `build/lib` 会把迁移前的 Consumer 模块带入 wheel，现改为从全新源码快照构建，
校验包内存在兼容命名空间且没有旧 `consumer/emt_*` 模块。在新虚拟环境安装锁定运行依赖，
以 Python `-I` 和不可用编译器运行离线自检，直接导入已安装的原生模块，确认 C++ 标准为 202002。

环境排查逐文件运行了 11 个文件。原候选线程/进程相关六个文件全部在单文件期限内完成，
本轮未复现此前的全量运行停滞；新增 15 秒 faulthandler 诊断与进程期限用于后续定位，未宣称根因已消除。
两项历史证据测试仍因以下文件缺失而失败，没有补造证据或改成跳过：

- `artifacts/acceptance/f0/2026-09-09-f0-quality-baseline.evidence.json`
- `artifacts/acceptance/f1/2026-09-09-f1-recovery-implementation.blocked.json`

排查另发现保留型订单簿 `_transition` 未同步父类撤单附加参数，导致 `request_cancel` 抛出 TypeError。
现已转发参数并新增回归：请求身份与原因持久化，撤单确认前保留预留，重复请求不追加事实，确认后才释放。
保留型订单簿和冷恢复测试已加入统一核心清单。

历史 106 处无理由 type-ignore 已整改：删除测试/工具中无效抑制，逐文件 AST 比较确认这些改动不改变运行语义；
生产代码现存抑制补充具体原因。检查范围覆盖三个包、测试与工具，Ruff、边界、敏感扫描、
type-ignore 原因检查、依赖锁与 Git whitespace 门禁均通过。

已实际观察到前一提交 `d67ecdb` 的远端 Windows/Linux Actions 成功：
[运行 45767709994](https://github.com/XunAn523/Xun-Trade-platform/actions/runs/45767709994)。
本阶段新工作流的远端结果另行记录，不将历史运行当作新工作流验收。

新基线提交 `cc28d32` 的双平台 Actions 已实际完成且成功：
[运行 45773288354](https://github.com/XunAn523/Xun-Trade-platform/actions/runs/45773288354)。
本地 Windows 与 WSL 隔离 wheel 验证也通过；WSL 首次从默认源下载依赖超过 300 秒期限，
保留该超时记录，随后使用清华镜像重试成功，依赖版本与哈希未改变。

## 2026-11-26：P0 容量测量方法

容量工具现支持开放环固定到达速率，按 `start + index / rate` 计时，不随前一请求完成时间顺延。
每个请求从计划到达计延迟，并报告发压迟到，避免把生成器追不上或批次等待排除在长尾之外。
提供独立进程的阶梯与突发重复试验、进程期限、源码及原生扩展哈希、存储挂载信息、进程 CPU 与峰值 RSS。
RSS 包含导入和有界测量历史，不能视为业务常驻内存。测量最多 100,000 请求，不作为长时间 soak。

所有试验核对发出 = 完成 + 拒绝、模拟 broker 调用 = 完成；拒绝不重发。
P99 只对完成样本计算，零完成的轮次不生成零延迟汇总；拒绝率、完成率和阈值内完成比例另列。
阈值内比例以所有发出请求为分母；多轮仅汇总每轮 P99 的 min/median/max。
吞吐区分发压窗口内完成与含排空的总完成，保留排空时长、排队、日志 fsync 和 gateway C0→C7 数据。

本阶段使用 25/50/100/200 请求每秒与突发五个负载点、每点 500 请求、三轮，
100 ms 仅作为实验观察阈值，未收到生产 SLA 或目标主机配置。
Windows 与 WSL 顺序测量，避免两组基准互相争用本机资源；实际目标 Linux 主机验收仍需独立执行。
