# XunTradeOMSEMS 验证记录

验证日期：2026-11-23（Asia/Shanghai）。本次只执行离线与本地测试，没有连接券商或提交实盘订单。

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
