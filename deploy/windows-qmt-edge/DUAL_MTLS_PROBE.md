# QMT 双通道 DISARMED 跨机联调

日期：2026-10-09。本轮交付的是可部署的**传输探针**，不是原生 QMT 交易服务装配或 P0 capability 通过。两端均使用仓库要求的 CPython 3.10 与依赖；Windows Edge 的 SQLite 继续执行 WAL 修复版本门禁。

## 1. 固定拓扑与边界

| 项目 | 本轮配置 |
| --- | --- |
| Windows Edge | `192.168.251.2:19443`，仅绑定专用直连接口 |
| Linux 客户端 | 源地址固定 `192.168.251.1`，主动建立 COMMAND / EVENT 两条独立 TLS 连接 |
| 认证 | TLS 1.3、CA 链、server IP SAN、双端 leaf DER SHA-256 pin；pin 不是 IP 哈希或 PEM 文件哈希 |
| Windows 入站 | TCP 19443，仅 LocalAddress `.2` / RemoteAddress `.1` |
| 公网 / legacy | 本轮不使用公网监听；不复用或修改旧 9550–9554 网关规则 |
| 本轮 QMT 内操作 | **无需运行任何 QMT 模型或文件**；此探针不打开 native DLL、ring、原生账户或交易 profile |

WINDOWS_EDGE 使用专用 SQLite outbox，LINUX_CLIENT 使用专用 SQLite FULL receipt journal 与 ACK cursor。二者都有进程排他租约和 probe 数据库标识，拒绝已有非 probe 数据库；不得填写真实 Edge `edge.sqlite` 或 OMS Journal 路径。Linux journal 是**传输探针日志**，不是已经装配好的 OMS mmap Journal observer。

测试帧使用 `QmtTransportProbeDeliveryV1`，生产 `QmtEventWireCodec` 会拒绝。COMMAND HEALTH 始终为 `ready=false / TRANSPORT_PROBE_DISARMED`，所有 COMMAND 均拒绝。EVENT HEALTH 的 `ready=true` 仅表示该测试 outbox 可读，不是账户 READY。无 capability 签发、无查询、无下单或撤单。

Windows 到 Linux 的 ping/SSH 可达不证明反向 TCP 已通过；Linux 端必须在 Windows 启动后检查 `.2:19443`。TTL 和端口相同也不能单独证明两个 IP 属于同一主机；Linux agent 应用本机接口清单确认实际地址。

## 2. 证书：各端保留私钥，只交换 CSR 和公有证书

2026-10-09 本轮公有证书与已填 Windows 配置已按用户授权提交：
[qmt-probe-windows-public-20261009.zip](../../artifacts/qmt-bridge-handoff/20261009/qmt-probe-windows-public-20261009.zip)。
Windows agent 可直接拉取 `dev`，核对包 SHA-256
`684f0e31284f6e3b5f909ce688abd8cb4a01fcea05451f4f31d39a81a74fccb0` 后解压并按包内 README 安装。
包内仅有 CA/双端 leaf 公有证书、配置、metadata 和说明；原 `windows-edge-key.pem` 必须保留，不重新生成。
两端 leaf 到期为北京时间 **2026-10-16 14:39:22**，包内状态为签发时的准备快照，不构成跨机验收通过。
本次入库范围仅限该公有包，CA 与 leaf 私钥仍不入库；下列生成流程适用于首次签发或后续新一轮签发，无需为使用现有包重复执行。

以下命令均在最新 `dev` 源码根目录，用已安装项目依赖的 Python 执行。Linux 路径由部署 agent 预先建立并赋予受控测试用户，目录 `0700`、私钥 `0600`；CA 私钥目录不属于运行服务用户。Windows 在生成前，将 `certs` 目录 ACL 收敛为测试运行身份、SYSTEM 和必要管理员，不给 Everyone/Users 读私钥权限。工具拒绝覆盖已有文件。

Windows（当前部署根 `D:\projects\TradeCore\qmt-edge`，源码位于其 `src`）：

```powershell
python tools/qmt_bridge_pki.py request --role windows-edge --directory D:\projects\TradeCore\qmt-edge\certs
```

仅把 `windows-edge.csr` 交给 Linux 证书签发操作者；`windows-edge-key.pem` 留在 Windows。

Linux / 受控签发侧：

```bash
python tools/qmt_bridge_pki.py init-ca --directory /var/lib/tradecore-qmt-probe/ca-private
python tools/qmt_bridge_pki.py request --role linux-client --directory /var/lib/tradecore-qmt-probe/certs
python tools/qmt_bridge_pki.py sign --ca-directory /var/lib/tradecore-qmt-probe/ca-private --csr /var/lib/tradecore-qmt-probe/certs/linux-client.csr --role linux-client --ip 192.168.251.1 --output /var/lib/tradecore-qmt-probe/certs/linux-client.pem
python tools/qmt_bridge_pki.py sign --ca-directory /var/lib/tradecore-qmt-probe/ca-private --csr /var/lib/tradecore-qmt-probe/incoming/windows-edge.csr --role windows-edge --ip 192.168.251.2 --output /var/lib/tradecore-qmt-probe/incoming/windows-edge.pem
```

将 `bridge-ca.pem` 公有证书放入两端 `certs`；将 `windows-edge.pem` 放入 Windows `certs`。通过已认证的交接渠道核对 CA 和 leaf 指纹。CA 有效期 30 天，leaf 最多 7 天且不超过 CA；续签后必须同步更新对端 pin。不要传递 CA/leaf 私钥，不把生成目录提交 Git。

分别对实际 leaf 执行：

```bash
python tools/qmt_bridge_pki.py inspect --certificate /path/to/leaf.pem
```

输出的 `certificate_sha256` 是对端配置所需 pin。`inspect` 也可用于 CA 指纹核对。工具本身不分发文件、不创建防火墙规则、不启动监听。

## 3. 填写两端配置

将 Windows `transport_probe_config.example.json` 复制到部署根 `config/transport-probe.json`；Linux 使用 `deploy/linux-qmt-bridge/transport_probe_config.example.json`，安装为 `/var/lib/tradecore-qmt-probe/config/transport-probe.json`。相对证书/数据库路径以**配置文件所在目录**解析。

Linux 生成两个独立随机标识，作为本轮共同 `scope_digest` 和 `outbox_epoch`，将相同值填入两端：

```bash
python -c 'import secrets; print("scope_digest=" + secrets.token_hex(32)); print("outbox_epoch=" + secrets.token_hex(32))'
```

这些是测试身份标识，不能复用真实 native scope/generation。固定 `event_count=8`，所有 `REPLACE_*` 必须填完。Windows 填 Linux leaf pin；Linux 填 Windows leaf pin。两端均保持 `mode=DISARMED`。

同一测试轮次重启必须保留两端数据库、scope、epoch、count、pin。替换数据库或改变身份时使用新测试目录和新的 epoch，不靠删 cursor 或覆盖数据库宣称恢复成功。

## 4. 启动与验收

Windows 部署操作者按批准的本轮配置新增精确规则（管理员 PowerShell；先确认同名规则不存在，存在时核对其实际范围）：

```powershell
New-NetFirewallRule -DisplayName 'TradeCore QMT DISARMED mTLS probe' -Direction Inbound -Action Allow -Protocol TCP -LocalAddress 192.168.251.2 -LocalPort 19443 -RemoteAddress 192.168.251.1 -Profile Any
python -m xuanling_tradecore.adapters.qmt.probe_deployment --config D:\projects\TradeCore\qmt-edge\config\transport-probe.json
```

要求 `TRANSPORT_PROBE_LISTENING`、`mutation_allowed=false`、`capability_accepted=false`；监听地址必须是 `.2`，不是 `0.0.0.0` 或公网地址。该命令保持前台运行；Ctrl+C 会先中断/回收连接线程，再关闭 actor 与数据库。不要运行旧 `qmt_edge_service.py` 来替代本轮入口。

Linux 基础轮次：

```bash
python -m xuanling_tradecore.adapters.qmt.probe_deployment --config /var/lib/tradecore-qmt-probe/config/transport-probe.json
```

首次要求退出码 0、`TRANSPORT_PROBE_COMPLETE`、`received=8`、`confirmed_cursor=8`；相同配置再次执行要求 `received=0`、`confirmed_cursor=8`。它仍检查两条通道和 EVENT outbox 末端 HEALTH。传输无隐式重连、无命令重试。

### 入站策略只读核对

出现“无 19443 精确规则但连接成功”时，在 Windows 源码根目录运行以下只读脚本，优先使用管理员 PowerShell：

```powershell
powershell -NoProfile -File .\deploy\windows-qmt-edge\scripts\inspect-probe-firewall.ps1
```

脚本输出实际地址/接口、网络 profile、防火墙 profile 的 Enabled/默认策略/接口排除、MpsSvc/BFE 状态、监听 PID，以及启用的入站 TCP/Any 且端口为 Any/19443/覆盖范围的 allow/block 候选规则，包含程序、服务、地址和执行状态。它不修改规则或服务。候选规则仍须结合当前接口/profile、程序和远端地址审阅，不自动宣称有效放行范围。

非管理员或查询失败时输出 `PARTIAL_REQUIRES_ADMIN_REVIEW` 和 `query_errors`；空候选列表不能作为无规则的证据。精确规则新增不收窄已有宽泛规则，需先核实实际放行来源，再按部署变更流程处理。不能凭 TCP/TLS 成功判定入站 ACL 已验收。

### 两个中断轮次

为 A、B 各复制一份 Linux 配置，只将 `database_path` 分别改为尚不存在的 `../data/probe-A.sqlite` 和 `../data/probe-B.sqlite`。仍使用同一个 Windows outbox 与相同身份；已 ACK 记录保留，可从零显式 replay。

```bash
# A：receipt 已 durable，但 ACK 尚未发送。
python -m xuanling_tradecore.adapters.qmt.probe_deployment --config /path/to/probe-A.json --interrupt-after 2 --interrupt-phase after_journal
# B：已收到 Edge ACK 回执，但本地 cursor 尚未提交。
python -m xuanling_tradecore.adapters.qmt.probe_deployment --config /path/to/probe-B.json --interrupt-after 2 --interrupt-phase after_ack
```

分别预期退出码 3、`confirmed_cursor=1`，状态为 `INTERRUPTED_AFTER_JOURNAL` 或 `INTERRUPTED_AFTER_ACK_BEFORE_CURSOR`。每轮在中断后停止并重启 Windows 探针，保留 outbox；Linux 再执行同配置、去掉故障参数，要求退出码 0、`confirmed_cursor=8`、`replayed=1`，receipt 表中始终只有 8 个唯一序号。两轮顺序执行，不同时运行多个 Linux 客户端。此为明确边界的中断/restart 演练，不是整机掉电或任意 crash matrix 验收。

### 负面检查与归档

使用独立配置副本/新 Linux journal 分别改变 server pin、scope、epoch，要求退出码 2、`TRANSPORT_PROBE_BLOCKED`，不能产生完成结论。不要改动当前正确配置或真实凭据。更换 client pin 的负面测试须在 Windows 测试配置副本中操作并重启探针，随后恢复正确配置。

归档两端实际 HEAD（应相同）、Python/SQLite 版本、公有证书完整 DER 指纹/有效期、监听与防火墙范围、每轮命令类型/退出码/脱敏 JSON、两次重启结果、摘要 digest。回报精确受测版本，不写“最新 dev 自动通过”。停止探针后按部署侧流程关闭该临时入站规则；保留证据与状态文件，不删除 legacy 规则。

验收上限：`DUAL_MTLS_TRANSPORT_PROBE_DISARMED`。实际跨机结果由部署侧填写；开发机回环测试不算已连接 `.1/.2`。真实 callback projector、生产 OMS Journal/cursor 装配、查询终态、capability 与恢复/性能仍需后续 P0–P3 工作。
