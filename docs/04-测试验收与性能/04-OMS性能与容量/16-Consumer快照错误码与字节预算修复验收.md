# Consumer 快照错误码与字节预算修复验收

本轮基于 `c542800` 的增量快照投影，修复最新代码复核发现的三个问题。QMT ABI-2 两轮生命周期证据仍以架构文档 17 §1.9 为准；本轮改动属于 Linux Consumer 快照接口。

## 修复行为

| 场景 | 修复前 | 修复后 |
| --- | --- | --- |
| 旧单帧快照超过容量 | reader 的 `SNAPSHOT_REQUIRES_PAGINATION` 被 projection 包装为 `CURSOR_RECOVERY_UNAVAILABLE` | 保留容量错误，经 Gateway/dispatcher 返回 `SNAPSHOT_REQUIRES_PAGINATION`；未知 reader 故障仍返回脱敏的恢复错误。 |
| 已授权分页发生容量或暂时不可用错误 | Gateway 尝试其他账户后统一返回 `CURSOR_INVALID` | 仅 scope/游标不匹配继续尝试账户；授权后的 `FRAME_TOO_LARGE`、`SNAPSHOT_NOT_READY` 等错误及 retryable 原样保留。 |
| 分页字节预算 | 按缺少 `event_kind`、嵌套 payload、封装及游标的五字段列表估算 | order event 与 wire 共用表示；最终按整个响应的 canonical UTF-8 字节校验，包含 request_id、快照元数据、page/replay cursor 和空页封装。 |

`page_payload_budget_bytes` 约束完整 JSON 响应载荷，不含固定 32 字节 wire header；等于预算允许，超过一字节即返回 `FRAME_TOO_LARGE`。预算作为内部 page policy 传递，不增加 wire 字段或修改协议 schema/hash。全局 frame 上限继续生效。

没有截断订单、静默缩页或推进客户端游标。已签名 page cursor 可重试；预算不够时需要使用较小 page_size 创建新快照，而不是把容量失败当成鉴权失败。合法的空页也必须容纳完整响应和 replay cursor。

## 本地回归覆盖

在 `3323f53`（已包含 UDS 公平调度）加本轮修改上验证：Consumer V2、QMT 及公共 Adapter 相关测试 **212 项通过、208 个子测试通过、10 项因当前 Windows 缺少 POSIX/SO_PEERCRED/recvmsg 能力跳过**；修改模块 Pyright 0 errors / 0 warnings，Ruff 通过。跳过项须在 Linux 部署测试侧补验。

- 真实 Gateway session、两个账户授权及 dispatcher 串联；目标在第二个账户时，第一账户 scope 不匹配不遮蔽后续结果。
- 旧快照容量错误与未知 reader 故障的分类；分页容量错误、暂时不可用的 retryable、无效游标隔离。
- 原先估算 119 字节的订单在 120 字节预算下正确拒绝。
- 中间页、终页、空页、Unicode/引号/反斜杠及长 replay cursor：以实际 `encode_frame` 长度验证预算恰好相等通过、少一字节失败。
- 现有 wire golden vectors、快照索引、Gateway、dispatcher 与相关 Consumer/QMT 回归；离线结果不替代真实 Linux UDS 部署验收。

## 部署测试侧执行

1. 在 Linux 测试环境拉取 `dev`，记录实际完整 HEAD，按现有部署方式更新 Consumer 服务；保留原状态和验收日志。
2. 对超过旧快照容量的测试账户调用 `GetAccountOrderSnapshot`，要求收到 `SNAPSHOT_REQUIRES_PAGINATION`，连接仍能处理后续请求。
3. 使用 `CreateOrderSnapshot` / `ReadOrderSnapshotPage` 验证普通分页、终页 replay cursor、空账户、同 cursor 重试和错误账户/篡改 cursor 拒绝。
4. 在测试配置中设置足够小的 page payload budget，验证已授权超限返回 `FRAME_TOO_LARGE`，而不是 `CURSOR_INVALID`；测量完整 JSON 响应 UTF-8 长度，不只测 orders 列表。需要通过现有构建工厂注入测试预算，不新增生产配置绕过门禁。
5. 运行本轮回归及 Linux `SO_PEERCRED` / `recvmsg` / UDS 集成测试；确认 snapshot 错误不会断开正常连接或进入订单副作用路径。
6. 归档 HEAD、请求类型、脱敏错误码、retryable、页数/字节数、完整测试结果和日志 digest，将验收摘要提交并 push 到 `dev`；不归档 bearer cursor、账户凭据或原始敏感订单内容。

本轮仅修复正确性和容量边界，不宣称混合负载隔离、延迟或吞吐达标；S5 性能验收继续依据文档 15 的独立负载与计时要求执行。

## Windows 复验记录（2026-10-08）

| 项目 | 结果 |
| --- | --- |
| 受测 HEAD | `397ad5bc51c3fece67ffab455a84fe7c15b06fdc`（`dev`，含 `1eb9c99` 合同文本哈希归一化、`49c6235` 跨主机合同/追踪排序校验及后续 perf/test 提交） |
| Windows 本机回归 | `210 passed, 9 skipped, 208 subtests passed`，0 失败 |
| 合同哈希 | `1eb9c99` 为 `_tree_hash` 增加 `normalize_text`（CRLF→LF 归一化）并在 `.gitattributes` 增加 `contracts/consumer/v2/**/*.json text eol=lf`；`test_declared_contract_hashes_*` 在 Windows 通过 |

平台跳过 9 项（均为 Linux-only，Windows 缺少对应能力）：

| 测试 | 原因 |
| --- | --- |
| `test_consumer_control_root_v2.py` | Windows 缺少 POSIX 模式与普通符号链接创建 |
| `test_consumer_reactor_v2.py`（3 项） | 需 Linux `SO_PEERCRED` |
| `test_consumer_uds_server_v2.py`（4 项） | 需 Linux `SO_PEERCRED` |
| `test_consumer_unix_v2.py` | ancillary 检查需 `recvmsg` |

结论：**Windows 合同与本机回归全绿**；Linux-only 项（UDS/`SO_PEERCRED`/`recvmsg`/POSIX 模式与符号链接）在本机平台跳过。开发侧已完成 Ubuntu WSL 补验；业务 Linux 目标机的部署侧验收（旧快照超限、分页预算、多账户隔离、cursor 重试、UDS 集成）仍需单独执行。
