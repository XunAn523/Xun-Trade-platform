# Linux 运行环境与 SDK 兼容性

> 状态：实施基线。`emt-gateway` 的部署、测试、性能证据与发布仅支持 Linux x86_64；Windows 不属于受支持平台。

## 1. 决策与边界

网关只维护**一个代码库**以及一套协议、风控、状态机、审计和测试。生产、仿真柜台、持续集成、性能验收和发布制品的唯一目标组合为：

```text
Linux x86_64 + CPython 3.10 + 供应方匹配的 EMT Python binding / 原生库
```

Windows 的旧 SDK 文件、旧代码路径和历史压测报告可保留为审计材料，但不构成当前支持、验收或发布依据。不得在 Windows 启动服务、生成发布制品、执行 EMT 验收或将 Windows 测试结果外推到 Linux。任何新文档、脚本或 CI job 均不得增加 Windows 运行步骤。

每个部署实例在项目根目录创建自身的 `.venv`；`.venv/` 是本机生成物，必须写入 `.gitignore`，不得提交或从一台机器复制到另一台机器。解释器固定为 **64 位 CPython 3.10**；即使更高 Python 版本能够导入 binding，也不得放宽生产基线。

```text
emt-gateway/                         # 同一 Git 工作树
  .venv/                             # Linux 实例专属；不提交
  emt_gateway/                       # 核心业务代码
  emt_gateway/platform_loader.py     # 唯一的 SDK 装载边界
  vendor/emt/
    linux-x86_64/                    # 只读 .so，按供应许可分发
  pyproject.toml                     # Python 依赖与 Linux CPython 3.10 约束
  uv.lock                            # 唯一、提交的精确依赖锁
  tools/capability_probe/
  deploy/linux/
```

`vendor/emt/` 可由受控制品仓库在发布时注入；若许可证不允许入库，仓库只提交哈希、版本和取得说明。无论来源如何，运行时都必须校验已装载文件的 SHA-256。

`uv.lock` 只需解析 Linux x86_64 的 CPython 3.10。运行和验收的唯一 Python 依据是项目 `.venv/bin/python`：它必须是 64 位 CPython 3.10；不得读取、校验或记录部署机发行版默认 `python3` 的版本。创建 `.venv` 所用的 CPython 3.10 可以由部署实例的项目受管运行时提供；`uv` 只同步该 `.venv` 的依赖，不能替换其中的解释器。

开发、测试和部署均从项目根目录创建 `.venv`，不使用全局 Python 安装第三方包：

```bash
# PROJECT_CPYTHON310 是该部署实例受管的 x86_64 CPython 3.10，不是系统默认 python3
"$PROJECT_CPYTHON310" -m venv .venv
uv sync --frozen --no-managed-python
```

人工调试可使用 `source .venv/bin/activate`；服务和 CI 必须直接调用 `.venv/bin/python`，避免受登录 shell 影响。

### 1.1 DISARMED scope profile

真实账户 profile 只保存在忽略的本地 `data/performance/config/emt/`；构造时使用项目 `.venv` 的离线工具。请求文件可包含账户 scope，私钥文件必须位于本地密钥目录且不得提交；生成的 profile 仅保存 Ed25519 公钥、签名、证书/撤销引用和 `approved=false, lifecycle=DISARMED`，不含私钥或凭据：

```bash
.venv/bin/python tools/performance/build_emt_disarmed_profile.py \
  --request ./data/performance/config/emt/<disarmed-signing-request>.json \
  --private-keys ./data/performance/secrets/<emt-scope-private-keys>.json \
  --output ./data/performance/config/emt/<signed-disarmed-profile>.json
```

构造器和 loader 均拒绝 `approved=true`、非 `DISARMED` lifecycle、重复账户/client scope、篡改签名和不受信 signer；构造器同样拒绝覆盖任何已存在的 profile 输出。生成 profile 后仍不能登录、查询、订阅、回放、下单或撤单；这些操作各自受后续 P0 与审批 gate 控制。

三个模拟盘 scope 的本地 signer 与模板可一次初始化；工具拒绝覆盖任何已有材料。默认生成普通、信用、期权 scope（`7001`、`7002`、`7003`），并将私钥只写入 `.local-secrets/`：

```bash
.venv/bin/python tools/performance/bootstrap_emt_disarmed_profile.py \
  --private-keys ./.local-secrets/emt-sim-disarmed-signer-v1.private-keys.json \
  --public-key ./data/performance/config/emt/emt-sim-disarmed-signer-v1.public.json \
  --revocations ./data/performance/config/emt/emt-sim-disarmed-revocations-v1.json \
  --request-template ./data/performance/config/emt/emt-sim-three-scope-disarmed-request.template.json
```

模板刻意不能直接签名：必须在本地以账户配置和 uniqueness-probe 证据替换 `__REPLACE_*__` 标记，再将 `template` 改为 `false`。这一动作不涉及 `approved=true`，生成结果仍只能是 DISARMED profile。

可用物化工具从既有模拟盘配置按 ordinary/credit/option 三个别名复制**账户标识**，并只写入新的 ignored request；它不会写入密码、不会加载 SDK，也会保留 `uniqueness_probe_succeeded=false`：

```bash
.venv/bin/python tools/performance/materialize_emt_disarmed_profile_request.py \
  --template ./data/performance/config/emt/emt-sim-three-scope-disarmed-request.template.json \
  --accounts-file ./data/performance/config/emt/emt-sim-accounts.json \
  --output ./data/performance/config/emt/emt-sim-three-scope-disarmed-request.json
```

物化后的 request 可交给构造器签名，但在独立 uniqueness probe 通过前仍无法激活 mapping 或打开 session。

生成 profile 后必须使用独立保存的 public-key 和撤销记录做第二次验签。profile 内嵌的 public key 仅用于发现文件意外损坏，**不**是可打开受控 session 的信任根；任何可能调用平台 session factory 的调用方都必须显式传入外部 `EmtEd25519ScopeOwnershipVerifier`。本地模拟盘可按以下顺序完成全离线 ceremony，生成的 acceptance 报告会省略账户号、密码、端点、私钥、公钥字节和签名：

```bash
.venv/bin/python tools/performance/build_emt_disarmed_profile.py \
  --request ./data/performance/config/emt/emt-sim-three-scope-disarmed-request.json \
  --private-keys ./.local-secrets/emt-sim-disarmed-signer-v1.private-keys.json \
  --output ./data/performance/config/emt/emt-sim-three-scope-disarmed-profile.json

.venv/bin/python tools/performance/verify_emt_disarmed_profile.py \
  --profile ./data/performance/config/emt/emt-sim-three-scope-disarmed-profile.json \
  --public-key ./data/performance/config/emt/emt-sim-disarmed-signer-v1.public.json \
  --revocations ./data/performance/config/emt/emt-sim-disarmed-revocations-v1.json \
  --output ./data/performance/config/emt/emt-sim-three-scope-disarmed-acceptance.json
```

验收器要求 profile 恰好覆盖 ordinary/credit/option 的 client scope `7001/7002/7003`、三个不同账户标识、`fengyu` owner、当前有效且未撤销的外部 signer，以及 `approved=false, lifecycle=DISARMED` 和 `uniqueness_probe_succeeded=false`。它拒绝自签名换钥、过期/撤销 signer、错误 scope 或已有报告输出。验收通过也不允许 activation、login、query、订阅、回放、下单或撤单；它只证明本地 DISARMED 文件的离线完整性和外部 signer 一致性。

## 2. Linux 装载契约

业务代码只能依赖 `native_api.py` 暴露的稳定 facade，不能直接导入 `vnemttrader`，也不能散布 `sys.platform` 判断。

| 项目 | Linux x86_64 要求 |
| --- | --- |
| Python | CPython 3.10 `.venv`。 |
| SDK 文件 | `vnemttrader.so`、`libemt_api.so` 及同目录依赖。 |
| 装载方式 | 仅为 EMT 运行进程设置 `LD_LIBRARY_PATH=<sdk_root>/lib/linux`；`<sdk_root>` 是包含 `lib/linux/vnemttrader.so` 的 SDK 根目录，`platform_loader` 只将该 `lib/linux` 目录加入 `sys.path`。不得把 SDK 根目录本身误设为 `LD_LIBRARY_PATH`。 |
| 系统依赖 | `glibc`、`libstdc++`、`libpthread` 满足供应包要求；启动前执行 `ldd`。 |
| 服务启动 | `systemd` 直接调用 `.venv/bin/python` 并显式声明 `LD_LIBRARY_PATH`。 |
| 持久化确认 | `fdatasync` 或 `fsync` 成功。 |

不得修改系统全局 `PATH`、`PYTHONPATH`，也不得将 SDK 复制进 `site-packages` 以“解决”导入问题；这样会使多账户、多版本并存时装载不可证明。加载失败、ABI 不匹配、依赖缺失、哈希不匹配或版本不匹配一律进入 `CAPABILITY_MISMATCH`，拒绝交易。

### 2.1 原生扩展、契约工具与性能证据

`native/fastspi_ring.c` 必须针对 Linux CPython 3.10 ABI 构建；生产 preflight 使用 `require_native_fastspi=True` 验证原生 ring 可导入和可用。Python fallback 仅允许功能测试，不构成性能发布证据。

`uv`、Ruff、Pyright、JSON Schema、OpenAPI、Schemathesis、Buf 和 AsyncAPI 都是构建/测试契约工具，在 Linux CI 中执行。`Buf` 与 `AsyncAPI` 仅在 JetStream/Protobuf 实际启用后才成为必需门禁；它们不进入 EMT 报撤或 callback 热路径。

Python 工具由 `uv.lock` 锁定；`uv`、Buf 和 AsyncAPI 校验 CLI 的版本、来源和 SHA-256 写入版本化 toolchain manifest。Linux kernel、`perf` 与 `ftrace` 版本写入性能报告。CI 禁止隐式下载未锁定的生成插件或以工具最新版替代已批准版本。

尾延时归因使用版本化 `perf`/`ftrace` profile，仅在批准的、无真实委托性能窗口采集 CPU、调度和存储证据，并输出脱敏 evidence manifest；原始 trace 受限留存，不写入 Git、Journal 或遥测 attribute。

## 3. 当前供应包的已知限制

Linux 交付包必须含 `vnemttrader.so`、`libemt_api.so` 和它们的运行时依赖。上线前在目标主机以隔离依赖路径完成导入、创建/释放 API、登录、回调和查询 P0 探针；不得只因系统存在 Linux 终端枚举就认定 binding 可用。

供应方 Python binding 不是当然的 API `2.27.0` 全覆盖结论。相对官方 86 个 `TraderApi` 方法和 69 个 `TraderSpi` 回调，当前已审计的 binding 差异为：

| 类别 | 2.27 必需但 binding 缺少 | 映射例外、额外/废弃 |
| --- | --- | --- |
| 主动方法 | `creditMarginAvlTransfer`、`queryCreditMarginAvlTransfer` | `registerSpi` 不直接导出，由 `TraderApi` 子类和 `createTraderApi` 完成注册；另有 `exit`、`queryStructuredFund`。 |
| 回调 | `onCreditMarginAvlTransfer`、`onQueryCreditMarginAvlTransfer` | `onQueryStructuredFund`。 |

上述保证金调拨能力必须报告为 `unsupported` 并拒绝调用，不能伪造为全覆盖。要满足[接口全覆盖矩阵](../02-接口与数据/02-EMT交易与账户接口全覆盖矩阵.md)，必须取得或重建同时暴露这两个方法及回调的 Linux Python binding，并重新完成 Linux P0。

## 4. Linux 发布门禁

Linux x86_64 在干净的项目 CPython 3.10 `.venv` 中执行以下步骤；任一项失败，制品不得晋升：

1. 创建 `.venv`，并只通过 `.venv/bin/python` 执行 `uv lock --check`、`uv sync --frozen --no-managed-python` 和 runtime preflight；记录该 `.venv` 的 Python 版本、`sys.implementation` 和架构。不得读取、校验或记录服务器默认 `python3`；缺失或过期 `uv.lock`、自动下载解释器或 `.venv` 解释器不为 CPython 3.10 均阻断发布。
2. 校验 SDK 二进制 SHA-256、文件权限与 `linux-x86_64` 目录；确认 `ldd` 将 `libemt_api.so` 解析到 `sdk_home`。
3. 运行无凭证导入探针、创建/释放 API 探针和 capability probe；真实登录、回调、查询探针仅在隔离测试账户执行。
4. 比较 `GetApiVersion()`、86/69 名称/签名映射、文件 SHA-256 和账户能力。差集、未知额外项或未声明的 `unsupported` 均使实例维持 `CAPABILITY_MISMATCH`。
5. 构建并导入 native `FastSpiMpscRing`，以 `require_native_fastspi=True` 运行 callback ingress 契约与压力测试；Python fallback 的成功不能替代该门禁。
6. 运行单元、组件与故障测试，以及 1 条/秒和 300/s 的无副作用性能报告；发生尾延时回退时附上 `perf`/`ftrace` evidence manifest。

每份 capability report 必须包含 `platform=linux`、架构、Python 可执行文件、Python ABI、SDK 目录、所有 SDK 文件哈希、动态依赖解析结果、`GetApiVersion()`、86/69 差异以及 `supported`/`unsupported` 原因。
