# Consumer 合同跨平台与 Linux 补验

## 2026-10-08 反馈处理

Windows 部署侧报告在 `b279d96` 上出现合同声明哈希不一致，且 Linux 专用接口测试跳过。复核确认：Consumer 合同 JSON 未固定 checkout 换行，声明混用了 CRLF/LF 基线。远端 `1eb9c99` 已补齐 `.gitattributes`，将文本身份统一为 LF 归一化哈希，binary golden 继续逐字节校验。

本轮在独立检出中继承该提交，保留原开发工作区；补充重建工具显式写出 LF，使旧 CRLF 工作区执行重建后也获得同样的资产字节。并增加 `core.autocrlf=true/false` 的全新 checkout 回归、旧工作区 CRLF 重建与二次重建幂等验证。内容漂移检查继续有效，只有文本换行差异被归一化。

统一后的声明（Windows 与 Linux 重建一致）：

| 项目 | SHA-256 |
| --- | --- |
| schema_hash | `125a02e150c4837f483af2ac874a72b9a2cc2372de356bdfde5696ff504e6117` |
| golden_vectors_hash | `37b51746015dd7e916c0f1f41fc7703ac7864876a0d773320a95c09fdb039bcf` |
| compatibility_matrix_hash | `9c28d36dff47e799c6c94f5a135c5bdc341972b3bb02e08d83544d37e56b8eb2` |

重算不要求特定 OS；必须使用同一套规范化规则。命令为 `python -m tools.contracts.build_consumer_v2_golden`，不得仅替换声明值来掩盖实际内容改动。

## Linux 补验发现与修复

开发机存在 Ubuntu WSL 和独立 CPython 3.10.18，本轮创建专用测试 venv 并运行 Consumer V2 套件；未借用 git SSH 服务作为 Linux 测试服务器，未修改现有 `/opt/xuanling-tradecore` 部署或其配置。

首轮 Linux 实测发现两项此前被 Windows 平台跳过的失败：

1. 旧快照容量测试使用 `object()` 充当 binding，却访问 `binding.account_id`。改为带所需字段的测试夹具，并将这个无 socket 的单元测试从 Linux-only 类分离，Windows 也能执行。
2. 公平调度需要先解码确定 lane 再排队，原 trace 却在解码后记录 `frame_ready_ns`，测试和统计报告仍假定 dispatch 在 decode 之前。现改为真实的 `frame_ready → frame_decoded → dispatch_started → schema_validated`；记录 `decode_before_dispatch=true`，报告按该标记统计 `frame_decode`、`ready_queue` 和 `session_and_schema_validation`。无标记的历史记录保留旧解释，不排序或改写错误时间对来制造通过结果。

## 回归结果与边界

验证基线为 `1a0b4db` 加本轮修改；验证范围为 Consumer V2 全套测试（`tests/test_consumer*_v2.py`），包含合同、错误码/完整响应预算、多账户隔离、cursor 重试、真实 AF_UNIX 服务、SO_PEERCRED、recvmsg、reactor 与分片 replay：

| 环境 | 结果 |
| --- | --- |
| Windows / CPython 3.10 | 78 passed，9 skipped，196 subtests passed；跳过项为 POSIX/真实 Linux socket 能力。 |
| Ubuntu WSL / CPython 3.10.18 | 87 passed，0 skipped，196 subtests passed。 |

这补齐了开发侧 Linux 接口与集成回归，不等于某台业务 Linux 服务器已经部署，也不是 S5 性能/混合负载验收。测试使用临时 Journal、测试身份与本地 socket，未连接实际券商账户或执行真实订单。

## 部署侧交接

- Windows agent：拉取最新 `dev`，重新执行合同与本机回归；Linux-only 测试按平台跳过并注明，不再要求 Windows 提供不存在的 SO_PEERCRED/recvmsg。可用 `python -m pytest tests/test_consumer_contract_checkout_v2.py tests/test_consumer_wire_v2.py -q` 专项确认合同修复。原 QMT ABI-2 验收上限保持 `PROBE_INIT_TIMER_STOP`。
- Linux 部署 agent：在指定业务测试机上记录实际 HEAD、Python/运行环境、服务配置摘要及脱敏结果，按文档 16 执行真实部署验证；开发机 WSL 结果不能替代目标机配置与服务验收。
- 任何新代码或验收摘要修改均提交并 push 到 `dev`；不提交 bearer cursor、私钥、登录信息或原始账户数据。
