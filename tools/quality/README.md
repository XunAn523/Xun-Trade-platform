# Linux 质量证据

本目录的工具只在 Linux x86_64、系统 CPython 3.10 的受控 runner 中运行。先用已核验的系统解释器创建 `.venv`，再执行：

```sh
uv sync --frozen --no-managed-python --extra test
uv export --frozen --extra test --no-emit-project --no-hashes \
  --output-file artifacts/quality/third-party-requirements.txt
uv run --no-sync pip-audit --strict \
  --requirement artifacts/quality/third-party-requirements.txt
uv run --no-sync cyclonedx-py environment --of JSON --output-reproducible --output-file artifacts/quality/sbom.cdx.json
```

`M0-01` 的包边界质量门是标准库脚本，可在同步第三方依赖之前执行：

```sh
python tools/quality/check_tradecore_boundary.py
```

它按 `tradecore-import-policy.v1.json` 验证 distribution、CLI、package discovery 和 import 方向：只有 `xuanling_tradecore.compatibility` 可以直接依赖旧 `emt_gateway`；`domain/consumer/oms/persistence/runtime` 等通用层不得依赖柜台 SDK 或 compatibility 层。违规会返回非零退出码并列出稳定 `TCB*` 错误码。

厂商 SDK 不进入 Git，也不允许被此工具导入。专用 EMT 原生 runner 在已配置 `EMT_SDK_ROOT` 和 `EMT_SDK_VERSION` 后生成二进制版本与 SHA-256 清单：

```sh
uv run --no-sync python tools/quality/emit_emt_sdk_manifest.py \
  --sdk-root "$EMT_SDK_ROOT" \
  --sdk-version "$EMT_SDK_VERSION" \
  --output artifacts/quality/emt-sdk-manifest.json
```

清单仅包含 SDK 根目录下原生二进制的相对路径、字节数与 SHA-256；不得包含账户、凭据、委托或回调载荷。SDK 升级、原生 wheel 变更或审计发现未处置高危漏洞时，质量任务不得晋升为发布证据。

`check_sensitive_content.py` 是 F0 的凭据误提交门禁：它扫描 Git 跟踪和非忽略的新文本文件，检测私钥头、明显的密码/API key/token 赋值、带凭据 URL 和 HTTP Authorization header。输出只包含稳定规则号、相对路径和行号，永不打印命中值：

```sh
python tools/quality/check_sensitive_content.py
```

该脚本是预防性扫描而非密钥管理系统；任何命中都必须移至 `.local-secrets/` 或受控部署凭据渠道，并在提交前完成轮换/人工复核。
