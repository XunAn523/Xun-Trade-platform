# XunTradeOMSEMS

基于现有 `emt-win-gateway` 优化的 **C++20 + Python OMS/EMS 交易中台**。
Python 承载订单领域、OMS/EMS 编排、持久化恢复与 EMT/QMT/Paper 适配；C++ 承载日志 CRC32C、
保持原协议的批量分片与有界并行整数计算。

源工程保持不变；本仓库本地目录为 `D:\荨安\XunTradeOMSEMS`。来源提交及导入文件哈希见
[provenance.json](docs/provenance.json)。本地密钥、账户数据、供应商 SDK、虚拟环境和历史运行产物未导入。

## 已实现

- **实际日志路径接入 C++**：`emt_gateway.journal.crc32c` 直接调用原生扩展，x64 自动检测 SSE4.2，
  不支持时使用 C++ 可移植算法。旧日志帧、Castagnoli 校验、损坏尾截断和 fsync 语义保持兼容。
- **EMS 批量分片**：`AccountSessionShardRouter.route_execution_ids()` 使用 C++ BLAKE2s-128，
  与原单笔 `tcshard1` 路由结果一致。原账户 fence、路由 epoch、精确撤单身份继续生效。
- **整数金额批量计算**：检查数量、金额、现金上限、订单上限和 int64 溢出，避免浮点金额；
  每行是独立快照，结果不作为共享现金预留或实际下单授权。
- **C++ 并行**：批量计算释放 GIL，最多 32 个 worker；拒绝超界批次和类型不符输入。
- **可验证构建**：pip 原生扩展、独立 CMake/CTest、跨后端一致性测试、Windows/Linux CI 和离线性能脚本。

完整设计、语言边界与多进程分片规则见 [HYBRID_ARCHITECTURE.md](docs/HYBRID_ARCHITECTURE.md)。
本次提供单机 C++ 并行和既有账户分片接口；跨机器计算 RPC、集群共识和自动迁移不在本版本实现范围。

## 构建和离线自检

使用 **CPython 3.10**。Windows 需要 Visual Studio 2022 C++ Build Tools（MSVC 与 Windows SDK）；
Linux 需要支持 C++20 的 GCC/Clang 和 Python 开发头文件。原生扩展编译失败会阻止安装。

Windows PowerShell：

```powershell
cd D:\荨安\XunTradeOMSEMS
py -3.10 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e . pytest hypothesis
.\.venv\Scripts\python.exe -m xuntrade_omsems self-check --require-native
```

Linux：

```bash
python3.10 -m venv .venv
.venv/bin/python -m pip install -e . pytest hypothesis
.venv/bin/python -m xuntrade_omsems self-check --require-native
```

自检仅使用临时目录和离线示例，不连接交易柜台。`uv.lock` 固定应用与可选依赖的解析结果，
需要锁定安装时使用 `uv sync --locked --python 3.10`；常规 pip 命令按 `pyproject.toml` 版本范围安装。

独立构建 C++ 核心：

```bash
cmake -S . -B build/cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build/cmake --config Release
ctest --test-dir build/cmake -C Release --output-on-failure
```

`XUNTRADE_BACKEND` 默认为 `native`，扩展缺失时启动失败。设置为 `python` 可运行诊断对照；
设置为 `auto` 可显式允许缺少扩展的兼容运行。`python -m xuntrade_omsems info` 显示实际后端。

## Python 调用

```python
from xuntrade_omsems.compute import assess_orders, backend_info, session_shards

print(backend_info())
indices = session_shards(("execution-1", "execution-2"), 3, workers=4)
# quantity, price_minor, available_minor, order_limit_minor
results = assess_orders(((100, 1250, 200000, 200000),), workers=1)
assert results == ((125000, 0),)
```

批量金额结果为 `(notional_minor, code)`；code：`0` 通过算术检查、`1` 非法数值、`2` 溢出、
`3` 超订单上限、`4` 超现金上限。数量和价格为正整数，金额统一使用调用方选定的最小货币单位。
无效或溢出行的 notional 为 `0`。接口每批最多 1,000,000 行、最多 32 个 worker。

现有 `xuanling_tradecore`、`emt_gateway` 包和 `xuanling-tradecore` CLI 保留兼容。
所有真实交易仍必须走已有 OMS/EMS、journal、账户执行权和券商适配器。

## 回归和性能

安装后执行核心回归（将 `python` 替换为当前虚拟环境解释器）：

```bash
python -m pytest -q tests/test_hybrid_compute.py tests/test_account_session_sharding.py tests/test_journal.py tests/test_gateway.py tests/test_account_execution_router.py tests/test_execution_scheduler.py tests/test_tradecore_oms.py tests/test_execution_session_runtime.py tests/test_account_ownership_fence.py
python -m tools.benchmarks.hybrid_compute --output benchmark-results.local.json
```

已测环境、通过/跳过数量及限制见 [VALIDATION.md](docs/VALIDATION.md)。基准原始结果位于
[docs/benchmarks](docs/benchmarks)。性能脚本包含 Python 调用与数据转换成本，比较 Python、
C++ 单线程和 C++ 多线程；不包含磁盘 fsync、网络或柜台延迟，也不代表实盘 C0→C7 验收。

导入的原测试还包含历史证据、SDK、PostgreSQL、Linux 进程和部署环境测试。完整运行前需按
原文档提供相应环境；`pip install -e '.[test,tooling]' -e tools/performance` 提供可选测试工具。
历史文档和 [UPSTREAM_README.md](docs/UPSTREAM_README.md) 仅反映源工程状态。

新的计算层和离线 journal 支持 Windows/Linux；原 EMT 实盘部署仍以 Linux x86_64 和官方
CPython 3.10 SDK 约束为准。没有实际账户、SDK 和现场验收时，本仓库不宣称实盘可用。
