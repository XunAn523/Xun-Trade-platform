# Xuanling TradeCore（玄灵交易核心）

面向内部策略与人工操作的 Linux x86_64 OMS + EMS 交易核心。EMT、QMT 与 PaperBroker 是可插拔的 BrokerAdapter profile；项目不提供行情订阅、行情缓存或策略计算。Windows 不属于支持、验收或发布范围。

当前 `emt_gateway` Python 包是冻结的 P0 EMT 兼容实现；新的领域、OMS、EMS 与运行时控制能力将进入 `xuanling_tradecore` 主包。两者在迁移期并存，旧包不再定义产品名称或新模块边界。

## 当前实现状态

已开始 P2 订单热路径基础实现：

- `emt_gateway/gateway.py`：订单与撤单的幂等状态机。命令必须先取得 `COMMAND_PREPARED` 的 durable journal 证据；一旦写入 `CALL_STARTED`，同一业务 ID 永不自动调用 EMT 第二次。
- `emt_gateway/native_api.py`：唯一的 EMT 调用适配边界。它接收已完成 P0 装载和登录的 API 对象，不在业务代码中导入供应商 SDK。
- `emt_gateway/platform_loader.py` 与 `emt_gateway/session.py`：从指定 Linux SDK 目录装载 binding，校验 CPython 3.10/可选 SHA-256，并按“创建 API → 软件版本/心跳/公共流订阅 → 登录”的顺序建立单账户会话。
- `emt_gateway/spi_bridge.py`：生成官方 69 个 `TraderSpi` override；仅复制订单/操作关联 ID 到 P2 有界 callback pipeline。
- `emt_gateway/query_registry.py`：按有符号 `int request_id` 管理查询，支持分页 `is_last`、单次响应、超时和冷却期；旧 binding 的无 request-id `QueryYesterdayAsset` 仅在同 callback 单飞时关联，避免错配。
- `emt_gateway/journal.py`：提供严格 write-through 的 `FileJournal`：二进制帧、CRC32C、提交标记和启动时连续前缀恢复；仅写入命令身份与 SHA-256 摘要，并在 Linux 上经 `os.fsync` 返回 durable 证据。`InMemoryJournal` 仍仅供测试。
- `emt_gateway/recovery.py`：使用部署注入的 Fernet 密钥加密保管命令 payload，配合跨进程 journal lease 生成恢复计划。`CALL_STARTED`/`SUBMIT_UNKNOWN` 只能人工或权威查询核对，绝不自动重发。
- `emt_gateway/capabilities.py`：在不登录、不调用 EMT 的前提下检查当前 binding 的官方 86 个 `TraderApi` 路由，缺失方法显式报告为 `unsupported`，`RegisterSpi` 显式报告为 SPI bridge adapter。
- `emt_gateway/facade.py`：完整登记官方 86 个主动接口；生命周期由 `EmtSession` 独占，查询统一管理 request-id，其余 direct 接口按经过 SDK 样例验证的 session 参数位置调用，变更型接口必须持有显式 permit。
- `emt_gateway/gateway.py` 的 `CallbackPipeline`：使用有界 `fastspi` MPSC ring（附带可选 C 原生实现），持久化事件后等待策略 ACK，并与 C0–C7 压测 trace 关联。生产部署可设 `require_native_fastspi=True` 拒绝 Python fallback。

当前 P2 已具备真实会话、全量 SPI bridge、86 项 facade 路由、strict FileJournal、加密 payload custody、跨进程 journal lease 与原生 fastspi 的接入点。仍缺逐接口业务字段校验/fixture、策略 TCP 数据面，以及缺失的两项 SDK binding；因此不能用于真实交易或宣称全接口性能通过。

## 本地验证

```bash
.venv/bin/python -m pip install -e .
.venv/bin/python -m unittest discover -s ./tests -v
.venv/bin/python -m unittest discover -s ./tools/performance/tests -v
```

生成 P4 预算模板（空预算和空审批不能通过验收）：

```bash
.venv/bin/emt-gateway-perf --driver baseline-template --output ./data/performance/plans/gateway/performance-baseline.json
```

可在不登录的情况下检查本机 SDK binding 覆盖；该检查不会访问账户、密码或交易服务：

```bash
.venv/bin/python -c "from pathlib import Path; from emt_gateway.capabilities import inspect_trader_api; from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings; sdk = EmtPlatformLoader(SdkLoadSettings(sdk_root=Path('EMT_python_SDK/emt_api_python'))).load(); print(inspect_trader_api(sdk.trader_api_type).as_dict())"
```

性能体系及 C0–C7 执行方法见 [全接口压测建设方案](./docs/04-测试验收与性能/03-EMT接口性能/01-全接口压测建设方案.md)。完整架构、协议和部署限制见 [文档中心](./docs/README.md)。
