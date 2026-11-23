# Adapter 配置控制面实施与上游联调方案

> 定档版本：Provisioning Control API `1.0`。本文件把 [ConsumerGateway V2 上游策略联调接口规范](./06-ConsumerGatewayV2上游策略联调接口规范.md) 的账户、密码与交易地址控制面，拆解为 TradeCore 本体的可实现模块、数据契约、联调步骤与集中验收。本文件及 `contracts/provisioning/v1/openapi.json` 是当前代码的控制面定档。

> 2026-09-17 代码交付状态：P0–P6 已完成仓库实现与无真实柜台副作用的集中合同/故障验收。实现包括严格 RFC 7516 compact JWE、短租约 SecretProvider、幂等 request/JTI/revision/binding 账本、TLS 1.3 mTLS HTTP/2 listener、双人审批、`ACTIVE_DISARMED` binding 发布、EMT 本机解封与 QMT Windows Edge-only 解封、Consumer UDS client/service 和 profile 退役 fence。实际主机的 CA、私钥、生产 SecretProvider、PostgreSQL repository、EMT/QMT 只读 probe 及性能基线仍是**部署证据**，不在仓库内伪造；它们也不会把 profile 升级成 `READY`。

## 1. 目标、边界与结论

目标是让**获授权的上游配置客户端**（不是普通策略进程）能安全地向 TradeCore 提交一个柜台账户的账号、密码、client ID 与交易地址，并在 `DISARMED` 状态下完成验证、审批和绑定发布。已发布 binding 随后可由普通策略通过 `ListAccounts` 返回的 `consumer_account_id` 选择；策略的 `SubmitSignal` 永远只含业务信号。

```text
配置客户端
  └─ HTTPS / HTTP2 / TLS1.3 mTLS + JWE ─► Provisioning Control API
       ├─ EMT：本机 Linux SecretProvider ─► EMT Adapter profile
       └─ QMT：opaque JWE mTLS 转交 ───────► Windows QMT Edge SecretProvider

策略客户端
  └─ Linux AF_UNIX / Consumer V2 ─► consumer_account_id ─► 固定 binding ─► EMS ─► Adapter
```

以下边界不可改变：

| 允许 | 永远禁止 |
| --- | --- |
| 配置客户端在独立控制面提交 JWE；TradeCore 将其转换为不可回读 secret reference。 | 在 `SubmitSignal`、Consumer event/cursor、OMS Journal、性能 trace、PG 业务表、Git、命令行、环境变量或日志中出现明文 credential/endpoint。 |
| 一个 consumer 通过多个受授权 binding 选择 EMT/QMT 等多个 Adapter。 | 一笔 signal 自由指定 `adapter_instance_id`、任意柜台地址、账号或 Adapter 类型。 |
| profile revision 的受控轮换、显式审批、对账后 retirement。 | 热路径自动切换 Adapter；用新 profile 的空查询否认旧 profile 的未知订单。 |
| `DISARMED` 下只读 capability/recovery 验证。 | Provisioning 操作自动进入 `READY`、自动登录、下单、撤单或解除现有发布门禁。 |

因此，这不是“策略下单 API 接受密码”。准确能力是：**上游配置系统用受控 API 配置 Adapter；上游策略用无密数据面调用已配置的 Adapter。**

## 2. 当前代码核验与改造缺口

| 当前组件 | 已有能力 | 本方案要补齐的能力 |
| --- | --- | --- |
| `xuanling_tradecore/consumer/bindings_v2.py` | 不可变 `ConsumerBindingSnapshotV2`、账户/策略授权和 `authorization_epoch`。 | binding revision 的持久化、控制面原子发布、profile revision 引用与旧 epoch 失效。 |
| `xuanling_tradecore/consumer/gateway_v2.py`、`dispatch_v2.py` | Consumer V2 的 8 个操作和不泄漏 Adapter 内部 ID 的 dispatcher。 | Linux UDS 常驻 service/client，及在 snapshot 中读取已激活 binding 的 runtime provider。 |
| `xuanling_tradecore/consumer/oms_operations_v2.py` | Journal-first OMS 准入，刻意不直接调用 Adapter。 | 由 EMS 正常 lane 消费已提交 execution，并经 runtime registry 构造指定 live Adapter。 |
| `xuanling_tradecore/consumer/unix_v2.py` | Linux UDS socket/peer credential listener 原语。 | service lifecycle、每连接 dispatch、速率/审计和 client SDK；不承载控制面。 |
| `xuanling_tradecore/ems/adapter_registry.py` | `VenueAdapterRegistry` 在 `DISARMED` 下选择精确 profile、限制 profile switch。 | 仅接受由已批准 profile revision 构建的 declaration、受控 secret lease factory、activation coordinator。 |
| `xuanling_tradecore/adapters/emt/profile.py` | EMT 受控 execution/read-only profile composition。 | 从 Linux SecretProvider 取得短期 lease 的 session factory；不得把 secret 写入 manifest。 |
| `xuanling_tradecore/adapters/qmt/profile.py` 与 `adapters/qmt/edge_*` | QMT bridge 合同与 Windows Edge P0 `PROBE` 基础。 | Edge 端 secret recipient/SecretProvider、受控 profile 状态与回传脱敏 evidence；P0 之外仍不可交易。 |

现有 `SignedReleaseManifestV1` 正确地不包含 credential 或 endpoint；新代码不得改变此规则。配置修订的非敏感摘要可被 release/preflight 引用，原始 credential 永远不能写入 release manifest。

## 3. 目标组件和责任划分

新增模块应保持依赖方向为 `control_plane → runtime/ems → adapters`，不得反向让 domain、OMS、Consumer V2 wire 或热路径导入 JOSE、HTTP、数据库驱动或 secret SDK。

| 模块（建议路径） | 责任 | 明确不做 |
| --- | --- | --- |
| `xuanling_tradecore/control_plane/provisioning_v1/contracts.py` | 不含 secret 的 request/response、角色、状态机、错误码、profile identity。 | JWE 解密、HTTP、SDK 登录。 |
| `.../jwe_v1.py` | 受限 JWE header/claim 验证、一次性 `jti`、payload digest。 | 日志 JWE 或返回解密明文。 |
| `.../secret_provider_v1.py` | `SecretProvider`、opaque `SecretReference`、一次性/短时 `SecretLease` protocol 与 fake provider。 | `read()`、`list()`、repr 中输出 secret。 |
| `.../profile_registry_v1.py` | profile/revision 状态机、不可变 revision、idempotency、retirement gate。 | 直接构造 native Adapter 或修改 binding。 |
| `.../control_ledger_v1.py` | 非敏感 request ledger、anti-replay/approval/审计摘要 repository protocol。 | 保存 JWE 字符串、密码、账号或完整 endpoint。 |
| `.../activation_v1.py` | `VALIDATED → ACTIVE_DISARMED` 的审批、binding snapshot 原子发布、epoch 递增、与 `VenueAdapterRegistry` 的 DISARMED 选择协调。 | 把账户标为 `READY`。 |
| `xuanling_tradecore/services/provisioning_api_v1.py` | mTLS HTTP2 adapter、输入大小/速率/内容类型验证、仅脱敏响应。 | 与 Consumer UDS 共用 listener、线程池、端口或认证。 |
| `deploy/linux-control-plane/` | systemd/ACL/trust-store/recipient key 配置模板和运行手册。 | 在仓库提交私钥、证书、账号或 endpoint。 |
| `deploy/windows-qmt-edge/` | QMT Edge 本地 recipient、opaque-envelope 接收与 Edge SecretProvider。 | 把 QMT 密码转回 Linux 或策略端。 |

## 4. 控制面合同至本体对象的映射

### 4.1 身份、传输与 request 规则

控制面仅接受 `HTTPS + HTTP/2 + TLS 1.3 mTLS`。服务端从证书链、SAN URI/DNS 和发行方映射出 `ControlPrincipalV1`，再由本地 allow-list 授予 `ADAPTER_PROVISIONER` 或 `APPROVER`。HTTP bearer token、Basic auth、WebSocket、明文 HTTP、Unix Consumer socket 和 mTLS 证书外的自报身份均不支持。

每个请求必须有 RFC 9562 UUIDv7 `request_id`、`Content-Type: application/json`、小于 64 KiB 的无压缩 JSON body。服务端应返回 `X-Request-ID`，但不得返回 client certificate subject、账户、endpoint 或 JWE 明文。控制面 `request_id` 与 Consumer V2 `consumer_request_id` 是不同 namespace。

```text
POST /control/v1/adapter-profiles:prepare
  ├─ request_id + mTLS principal + outer payload digest
  ├─ JWE header/claims/replay validation
  ├─ local/remote SecretProvider.put_sealed(...)
  ├─ profile revision PREPARED + non-secret ledger
  └─ response profile_id/revision/digests/state only
```

`credential_envelope` 的 JWE 规则以接口规范第 7.3 节为准：`RSA-OAEP-256`、`A256GCM`、唯一 `jti`、最多五分钟有效期、`iss` 等于 mTLS principal、`aud` 等于实际 recipient target。拒绝 `zip`、嵌套 JWE/JWS、未知 `crit`、多 recipient、缺失/过期 `kid`、算法降级和已消费 `jti`。Ciphertext/JWE 本身也不入库；仅保存其 SHA-256 和 `kid`。

### 4.2 领域对象

```text
AdapterProfileId (稳定非敏感逻辑名)
  └─ AdapterProfileRevisionV1 (不可变、单调 revision)
       ├─ AdapterProfileIdentityV1
       │    adapter_kind, venue_id, adapter_instance_id, account_alias
       ├─ SecretReference (opaque provider ref；不可反向读取)
       ├─ ProfileMaterialDigest
       │    credential_digest, endpoint_digest, JWE kid, mapping/capability hashes
       ├─ ProfileState
       └─ approval / validation / retirement evidence IDs

ConsumerBindingRevisionV1
  └─ binding_id, consumer_id, consumer_account_id, strategy scopes,
     profile_id, profile_revision, authorization_epoch, effective interval
```

`account_alias` 是显示/审计别名，不得等于柜台账号。`adapter_instance_id` 必须每 revision 新建且永不复用；相同账号不同 Adapter、相同 Adapter 轮换账号、或相同 profile 轮换交易地址均是新的 revision，不能覆盖旧 row。

`SecretProviderV1` 最小 protocol：

```python
put_sealed(*, target, jwe_compact, expected_claims) -> SecretReference
lease(*, reference, purpose, ttl_s) -> SecretLease
revoke(*, reference, reason) -> None
```

只有对应 Adapter 会话工厂可调用 `lease`；lease 最长 60 秒、单 purpose、不可序列化、不可 repr、不可跨账户/Adapter instance 使用。provider 可按宿主实现 OS key store、HSM、Vault agent 或受限本地文件，但必须给出相同 contract 和审计摘要。任何 `get_secret`/`export` 接口都是禁止的。

### 4.3 非敏感持久化模型

控制面存储与 OMS/订单表分开。推荐 PG migration 使用以下表；其中不存储可逆 secret：

| 表 | 主键/关键字段 | 保存内容 |
| --- | --- | --- |
| `adapter_profiles` | `profile_id` | adapter kind、venue、受限 alias、当前 revision、lifecycle 摘要。 |
| `adapter_profile_revisions` | `(profile_id, profile_revision)` | instance ID、`secret_provider_kind`、opaque reference ID、JWE kid/digest、credential/endpoint digest、mapping/capability/release digest、state、时间。 |
| `control_request_ledger` | `request_id` | mTLS principal ID hash、payload digest、JWE `jti`/digest、结果 code、时间；唯一约束 `(jti, recipient_target)`。 |
| `control_approvals` | `approval_id` | approver principal、profile revision、审批内容 digest、有效期、签名/证据 reference。 |
| `consumer_binding_revisions` | `binding_revision_id` | 绑定身份、profile revision、authorization epoch、发布/撤销时间；不存策略 secret。 |
| `profile_validation_evidence` | `evidence_id` | capability/recovery/host-health 的脱敏摘要、状态和 artifact digest。 |

`credential_digest`/`endpoint_digest` 使用域分离哈希，例如 `SHA-256("tradecore/endpoint/v1\0" + canonical bytes)`；它们用于变更检测，不作为安全证明，且不得被 API 当作可逆识别信息。数据库备份、PG logical replication 与故障日志同样不得出现 ciphertext 或明文。

### 4.4 profile 状态机和路由不变量

```text
ABSENT --prepare--> PREPARED --validate--> VALIDATED --activate--> ACTIVE_DISARMED
                          └───────────────reject──────────────────> REJECTED
ACTIVE_DISARMED --retire--> RETIRING --all recovery gates--> RETIRED
```

- `prepare` 只创建新的 revision，写入 secret reference；不加载 SDK。
- `validate` 在该 revision 的账户隔离进程中执行只读 probe，并绑定 capability/mapping/scope ownership、release、host-health 与恢复证据。缺口是 `REJECTED`，不降级为可交易。
- `activate` 需要独立 approver，调用 `VenueAdapterRegistry.select(..., account_lifecycle=DISARMED)`，然后用一个事务发布 `ConsumerBindingSnapshotV2` 的 successor 和更高 `authorization_epoch`。
- Consumer session 在每个请求校验当前 epoch；旧 epoch 只能查询既有审计，不能创建/撤销新意图。`ListAccounts` 不返回尚未 `ACTIVE_DISARMED` 的 profile。
- `retire` 首先拒绝新 signal；旧 profile 必须无 working、`SUBMIT_UNKNOWN`、未归属 external order、未释放 reservation 和未完成 recovery。通过后先撤 binding、再 revoke secret reference、最后状态为 `RETIRED`。

一个 consumer 可以有多个 active binding，例如 `consumer_account_id=alpha-emt` 和 `consumer_account_id=alpha-qmt`。普通策略只能从 `ListAccounts` 返回项中选择，`ConsumerAccountBinding` 固定映射 `(account_id, venue_id, adapter_instance_id)`；所有 target runtime lookup 都由该三元组加 revision 完成。不得以策略字段、host、账号或 adapter kind 重新路由。

## 5. EMT 与 QMT 部署拓扑

### 5.1 EMT Linux

EMT 的 recipient target 例如 `linux-emt-secret-provider/<host>/<service-account>`。Linux Provisioning API 在同一受限账户域调用 provider 的 `put_sealed`；`EmtExecutionSdkSessionFactory` 只在已通过 activation/release gate 的账户进程内 lease。SDK 的 login 参数在 native boundary 可短时存在，但不能被保存到 Python log/exception、环境变量、systemd `Environment=` 或 profile/manifest。

### 5.2 QMT Windows Edge

QMT recipient target 必须是例如 `windows-qmt-edge-secret-provider/<edge-id>`，而不是 Linux Control API。Linux Control API 仅验证 JWE protected header、mTLS caller、outer identity 与 target，随后通过专用 mTLS Edge management channel **原样转送** ciphertext。Windows Edge 解封并将 secret 放入本机 provider，Linux 最多保存 returned opaque `edge_secret_reference`、profile revision 和 digests。

QMT Edge 的 `validate` 是由 Windows 本地受限任务调用 Big QMT/Native Engine 的只读/`PROBE` 行为，结果回传脱敏 evidence。P0 之外的 QMT trade/cancel/query 都继续 capability-gated；收到 profile 不等于可交易。

## 6. 分阶段代码实施与联调顺序

每一个 P 包完成全部代码后才做该包的集中验收；不得用真实 EMT/QMT 凭据作为单元测试 fixture。

| 包 | TradeCore 代码交付 | 上游/环境联调 | 包结束验收 |
| --- | --- | --- | --- |
| P0 合同 | `contracts.py`、JSON Schema、错误码、canonical digest、OpenAPI/示例；更新 Consumer V2 规范。 | 配置客户端可用 mock recipient 生成 JWE，策略 client 仍只走 fixture。 | schema/golden vector、拒绝明文/未知字段/算法降级，敏感扫描零命中。 |
| P1 领域与耐久 | profile/revision/ledger/approval repository protocol、内存 fake、PG migration 和 anti-replay/idempotency。 | 使用 fake provider 与临时 key；不连接柜台。 | revision 不可覆盖；同 request 幂等、异 payload 冲突；过期/重放/跨 target JWE 拒绝。 |
| P2 服务与密钥 | HTTP2/mTLS service、recipient keyring、SecretProvider lease/revoke、systemd/ACL/trust store。 | 测试 CA/测试证书；真实 host 仅部署 `DISARMED` service。 | 未认证/角色错/大包/明文字段/日志注入拒绝；secret 不在 API/DB/log/JWE archive。 |
| P3 EMT 激活 | validation coordinator、`VenueAdapterRegistry`/release/host-health/EMT session factory seam、binding successor publisher。 | 用 EMT 模拟盘只读 capability probe；不下单。 | binding 仅在 `VALIDATED + APPROVED + DISARMED` 发布；旧 epoch submit 被拒绝；C0–C7 基线不劣化。 |
| P4 Consumer 组合 | UDS service/client、OMS→EMS dispatch composition、runtime Adapter registry provider、account list projection。 | 策略 `ListAccounts → snapshot → PollEvents → SubmitSignal` fixture；EMT 另行受控验收。 | 同一 `strategy_order_id` 重试零重复 native call；跨 binding 读取/撤单失败；多 binding 正确隔离。 |
| P5 QMT Edge | opaque relay、Windows recipient/provider、Edge validation/evidence、QMT binding capability gate。 | 仅 QMT P0 `PROBE_READY_DISARMED`，随后按 P1–P4 逐项解除。 | Linux 无 QMT secret；edge target 不匹配失败；未验收 QMT 不出现在可交易列表。 |
| P6 退役与恢复 | retirement coordinator、recovery evidence gate、release/binding rollback fence、监控告警。 | profile 轮换、范围耗尽、旧 release rollback、模拟 crash/restart。 | 旧 unknown/working 存在时 retirement 拒绝；secret revoke 后不得重新 lease；恢复审计可追溯。 |

P3/P4 的真实交易验收仍依赖既有 `DISARMED`、scope ownership、人工批准和 EMT 权威恢复限制；本控制面不能绕开或弱化它们。

### 6.1 2026-09-17 集中实现与验收记录

| 包 | 实现状态 | 已完成的仓库验收 |
| --- | --- | --- |
| P0 合同 | 已完成 | OpenAPI、严格 outer request、唯一 JWE carrier、RFC3339 及不返回 secret 的 receipt 合同。 |
| P1 领域与耐久 | 已完成 | profile/revision、request/JTI 幂等、approval 与 binding repository protocol、PostgreSQL DDL、secret 写入后 registry 故障的同请求安全重试。 |
| P2 服务与密钥 | 已完成 | 独立 TLS 1.3 mTLS/ALPN `h2` listener、临时 CA 双向握手、大小限制、无 HTTP/1.1 fallback、租约/revoke 与部署 systemd/ACL gate。 |
| P3 EMT 激活 | 已完成 | `VALIDATED + 独立审批 + DISARMED` 才可发布 successor；selection gate 和 exact runtime registry 不接受 host/账号/策略字段动态路由。 |
| P4 Consumer 组合 | 已完成 | 常驻 UDS lifecycle/client、Journal commit 后仅唤醒 EMS lane、重复策略单不重复唤醒；控制面不进入 C0–C7 工作线程。 |
| P5 QMT Edge | 已完成 | Linux 对 QMT envelope 只 opaque relay；Windows `QmtEdgeCredentialReceiverV1` 才可解封/存储/撤销，target/replay 不符 fail-closed。 |
| P6 退役与恢复 | 已完成 | working/unknown gate、撤 binding→revoke→retire、lease 拒绝，以及 binding commit 与 snapshot publish 之间中断后的同请求恢复。 |

上述验收不包含真实 EMT/QMT 登录、查询、下单、撤单或网络侧部署操作；这些操作仍必须遵循各 Adapter 已有的模拟盘、scope、人工批准和 `DISARMED` 验收门禁。

## 7. 上游开发者联调手册

### 7.1 两种 client，两个权限域

| client | 使用场景 | 调用接口 | 可接触的内容 |
| --- | --- | --- | --- |
| `TradeCoreProvisioningClient` | 运维/账户接入平台配置或轮换柜台 profile。 | HTTPS/mTLS + JWE Control API。 | 仅在本地构造 JWE 的短时明文；服务端 response 无 secret。 |
| `TradeCoreConsumerClientV2` | 策略运行期选择已批准账户、提交/查询/回放信号。 | Linux UDS Consumer V2。 | `consumer_account_id`、业务信号、订单/事件脱敏视图；绝无密码、账号、地址。 |

同一程序若兼具两种责任必须进程隔离、不同 Unix UID 和不同证书；策略 worker 不能持有 `ADAPTER_PROVISIONER` 证书或 recipient public key 自动配置权限。

### 7.2 Provisioning 正常序列

```text
1. ProvisioningClient --mTLS--> GET credential-recipient(target)
2. ProvisioningClient --local JWE encrypt--> credential envelope
3. ProvisioningClient --mTLS--> prepare(profile identity + JWE)
4. TradeCore/Edge --> PREPARED (no SDK session)
5. Provisioner + Approver --mTLS--> validate
6. TradeCore/Edge --> VALIDATED + evidence digest
7. Approver --mTLS--> activate binding
8. TradeCore --> ACTIVE_DISARMED + authorization epoch successor
9. Strategy --UDS--> ListAccounts/snapshot/PollEvents/SubmitSignal
```

若任一步 timeout，client 使用相同 `request_id` 查询或重试；不得在不明结果下生成新的 profile ID 或重复提交 JWE。`prepare` 的 response 是接收/耐久结果，不是 SDK login 成功、柜台连通、账户可交易或订单可提交的声明。

### 7.3 示例（均为占位值，不含真实 credential）

```http
POST /control/v1/adapter-profiles:prepare HTTP/2
Content-Type: application/json
X-Request-ID: 018f0000-0000-7000-8000-000000000001

{
  "request_id": "018f0000-0000-7000-8000-000000000001",
  "profile_id": "alpha-emt-sim",
  "adapter_kind": "EMT",
  "adapter_instance_id": "alpha-emt-sim-20260917-r1",
  "account_alias": "alpha-sim",
  "credential_envelope": "<JWE compact serialization>",
  "expected_credential_kid": "linux-emt-2026q3-k1",
  "not_after": "2026-12-31T00:00:00Z",
  "approval_reference": "change-000001"
}
```

```json
{
  "request_id": "018f0000-0000-7000-8000-000000000001",
  "profile_id": "alpha-emt-sim",
  "profile_revision": 1,
  "adapter_kind": "EMT",
  "state": "PREPARED",
  "credential_digest": "<sha256>",
  "endpoint_digest": "<sha256>",
  "approval_required": true
}
```

响应示例刻意不含 profile endpoint、柜台账号或任何 JWE 字段。策略只在 activation 后看到例如 `consumer_account_id: "alpha-emt"`。

## 8. 集中验收、性能和故障矩阵

| 类别 | 必测证据 | 通过条件 |
| --- | --- | --- |
| 合同 | HTTP/OpenAPI、JWE vector、schema、错误 code、ID canonicalization。 | 所有合法/非法输入 deterministic；升级不破坏 Consumer V2 wire。 |
| 认证授权 | 无证书、错 CA、过期证书、错误 role、证书 subject/`iss` 不符。 | 全部 fail-closed；审计仅记录 principal hash/reason。 |
| Secret | 明文 JSON、JWE 重放/过期/错 target、未知 kid、日志/exception/PG/manifest 扫描。 | 明文和坏 envelope 零写入；不出现 credential/endpoint/ciphertext。 |
| 多 Adapter | 同一 consumer 两个 binding，跨账户 get/cancel/submit、profile switch。 | 只有已授权 `consumer_account_id` 能路由；没有动态 adapter/host 覆盖。 |
| 生命周期 | validate fail、approval 缺失、旧 epoch、working/unknown 时 retire、secret revoke。 | 状态机不能跳跃；所有危险操作 fail-closed。 |
| 恢复 | 进程在 prepare、validate、activate、retire 中终止后重启。 | request ledger/revision/approval 一致；不丢失 binding、不会重复 secret 写入或自动交易。 |
| 热路径隔离 | P0–P2 控制面开启/关闭的 Consumer/EMS 低并发和 499 burst 对比。 | 不共享 C0–C7 worker/端口/CPU；在固定环境基线内无统计显著回归；控制面故障不阻塞已激活账户。 |

所有包含真实 EMT/QMT 的检查继续遵守现有模拟盘、批准、scope、查询权威和 `READY` 门禁。配置面验收通过只证明“可安全配置并发布 `ACTIVE_DISARMED` binding”，不构成真实交易、自动恢复或全账户 `READY` 的证据。

## 9. 交付判定

P0–P6 的仓库实现已经完成。以下条件同时满足，才可将其宣布为某一具体主机上的**已部署控制面服务**：

1. Provisioning API 的 mTLS/JWE/anti-replay/SecretProvider/profile revision/approval/retirement 合同和实现全部通过；
2. EMT 与 QMT 分别在正确宿主保存 secret，Linux 不持有 QMT password；
3. Consumer V2 UDS service 与 EMS runtime composition 真实按 binding revision 路由多个已验收 Adapter；
4. 任何明文 credential/endpoint、动态 adapter 覆盖、未审批 activation、旧 epoch 和不完整 retirement 均 fail-closed；
5. 控制面上线前后，固定环境下既有 C0–C7、OMS/EMS/策略 TPS/QPS 统一压测无回归，并归档原始证据。

在完成该主机部署证据前，上游开发者可据第 7 节使用 mock/fake recipient 和 Consumer V2 fixture 联调，不得以“代码和 OpenAPI 已存在”假设某台机器上的生产控制端点已可用。
