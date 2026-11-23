# TradeCore QMT Adapter 未完成能力开发、实施与验收计划

> 2026-10-12 选路更新：用户确认国金大 QMT 内置 Python 将限制文件读写和外部连接，MiniQMT 已停用。旧读盘 loader / 文件映射 ring 不再作为默认后续部署路径；命名共享内存仍须券商确认与升级版本无交易副作用探针验证，见 [国金大 QMT 受限后的桥接替代方案](./19-国金大QMT内置Python受限后的桥接替代方案.md)。本更新不覆盖历史验收结果，也不表示替代桥接已实现。

> 状态：**Windows ABI-2 本机 DISARMED 生命周期（init、持续 timer、停止静默、restart）已完成两轮实测，验收上限 `PROBE_INIT_TIMER_STOP`，见 §1.9；P0 capability 整体验收尚未完成。** 2026-10-09 已交付双通道 DISARMED 传输探针及证书/部署入口（§1.10），开发侧 Windows/Ubuntu WSL 回环回归通过；真实 Linux–Windows 双通道基础传输及三项负面检查已通过（§1.12），A/B 两轮 Windows 进程重启续传均已通过（§1.13–§1.14）；Windows 报告存在 Python 宽泛放行规则，入站 ACL 收敛与复验仍待完成。订单回调、查询完整性、route/policy、签名 capability manifest、生产 callback projector/OMS Journal 装配、恢复与性能仍未完成对应验收。本文件是 [QMT Windows 边缘适配定档](./16-QMT-Windows边缘适配定档.md) 的执行计划，不改变 EMT 已验收结论，也不授权 QMT 真实下单。

> 计时原则：每一条 QMT 业务链路均必须产出逐笔、可关联、脱敏的穿透时间点。命令/回调链路统一使用 `C0`–`C13`；恢复使用 `R0`–`R8`；故障停单传播使用 `H0`–`H4`。没有原始时间对、完整性状态或时钟语义的数值不得进入性能结论。

## 1. 范围、完成定义与当前基线

本计划只完成一个与 `EmtBrokerAdapter` 同层的 `QmtBrokerAdapter`。它不新建 OMS、EMS、策略框架或第二套账户账本。Linux 上的 TradeCore 保持订单状态机、EMS 准入和 Journal-first 事实链；Windows 上的 Edge 负责边界认证、持久化幂等和可靠投递；Big QMT 内置 Python 只在官方允许的上下文中顺序调用原生交易函数。

```text
策略 / 消费端 → OMS → EMS → QmtBrokerAdapter → mTLS → QmtTradeCoreEdge
                                                        │
                                            Edge SQLite / SPSC rings
                                                        │
                                      Big QMT QmtNativeEngine → QMT 原生交易
```

### 1.1 已实现的内部代码边界

以下内部合同已经离线验证；部署侧实测范围另见 §1.2 和 §1.7。本机健康帧与本机回环 mTLS 合同测试均不代表完整 QMT capability、跨机连接或生产验收。

| 范围 | 已有实现 | 当前结论 |
| --- | --- | --- |
| Linux Adapter | `QmtBrokerAdapter`、typed bridge port、scope/profile fence、observer 先挂接、delivery ACK、事件去重/冲突 fail-closed | 可作后续 bridge 的唯一 OMS/EMS 入口；mutation 默认关闭。 |
| P0 activation / ABI | 签名 `QmtCapabilityManifestV1`、Ed25519 trust/revocation 校验、mapping/scope/Edge/route/ABI 绑定、固定 header/frame/CRC/commit ABI | 仅离线合同。没有真实 P0 evidence 时不得激活 production profile。 |
| Edge durable 核心 | scope-local `QmtEdgeStore`，request sequence、idempotency conflict、`PUBLISH_PREPARED`/call 状态、unknown-effect、event outbox/ACK；`QmtNativeEngineClient` 先标记后发 ring、先落 outbox 后返回 callback | SQLite/ring crash-boundary 合同已离线验证；部署侧报告对齐修复后跨进程 mmap 可见、真实 QMT 健康帧已持久化。真实业务 callback、跨机 mTLS 和故障矩阵尚未验收。 |
| Windows P0 部署 | 64-bit Interlocked DLL C 源码、固定 file-backed command/event ring、Big QMT Python 3.6 Native Engine、SHA-256 固定的 Big QMT 策略 loader、Edge CPython `PROBE` 与严格配置/SQLite 运行时预检 | 本机 `PROBE_READY_DISARMED` 与真实 Native Engine `PROBE_INIT` 已据部署侧报告通过，见 §1.7；默认 `PROBE`，本轮无提交、撤单或查询副作用。P0 capability 其余能力仍待验收。 |
| P1 command transport | TLS 1.3 双向 CA 验证 + 对端叶证书 SHA-256 pin、scope HELLO fence、canonical wire envelope、Windows command service、Linux `QmtMtlsBridgePort` | 离线协议测试通过；真实证书、Windows listener、断线/resume/event stream 与 ACK 尚未验收。 |
| P2 native encoder | P0-policy-bound `SUBMIT/CANCEL/QUERY` fixed payload encoder，先 durable `PUBLISH_PREPARED` 再 ring publish，Python 3.6 Native Engine QPC timepoints 与 `PROBE_INIT` health frame | 编码器无任何猜测的 QMT 枚举；P0 事实未填充时保持不可用，callback/query row 投影未实现。 |
| 事件归一化 | Edge event → `BrokerEvent`，source identity / hash / callback-gap 合同 | 不泄漏原生对象；尚未接到真实 callback。 |
| 测试夹具 | fake profile、adapter/edge-store 单测 | 仅证明合同，不得作为 P0、吞吐或恢复证据。 |

### 1.2 2026-09-17 Windows Edge P0 部署预检证据

本次仅验证专用 Windows Edge 进程能够在严格配置下创建其本机耐久边界；下列记录均为脱敏部署证据，未记录地址、账户、订单、凭据、私钥或原始命令载荷。

| 项目 | 已验证结论 |
| --- | --- |
| 源码供应链 | 专用 checkout 以 Git SSH 拉取 `dev` revision `26492b12f98286b66f0e66a81f0e7776d03ef7c9`，工作区干净。任何后续 P0 步骤前必须快进到当前 `dev`，不得手工复制源码。 |
| Edge 运行时 | 独立 CPython `3.10.11` virtual environment；实际导入的 SQLite 为官方核验的 `3.53.4`，符合受支持 WAL 修复版本范围。 |
| 本机耐久边界 | 已创建两个 `1024 × 512` 槽的固定 ring 与 WAL 状态库；预检输出为 `PROBE_READY_DISARMED`、`mode=PROBE`、`mutation_allowed=false`。 |
| 原子依赖 | 已构建 x64 atomic DLL，并记录 SHA-256 `81d4bb2ff4672a249fe0ca6626922f0574bce8f7eda5491ce2cd239caa0f3caa`。 |
| 权限边界 | 管理员与系统拥有完全控制；部署/QMT 同一受限身份拥有修改；普通 Users 仅可读执行；已移除 Authenticated Users 的写权限。 |
| 明确未发生的副作用 | 未启动 Big QMT 模型、event monitor、mTLS service 或 listener；未登录、查询、下单或撤单；未创建证书或开放防火墙。 |

该预检**不是 P0 capability 通过**：尚未证明 Big QMT 的 `run_time`、Native Engine 映射打开、Interlocked/alignment、route 字段、回调、查询 completion 或任何 C0–C13 时间对。下一步仍是 README 所定义的受控 `PROBE` Native Engine 启动和无副作用 health-frame 采集；P0 未通过前，production profile 固定为 `CAPABILITY_UNAVAILABLE`。

### 1.3 未完成能力总表

| 功能包 | 未完成交付物 | 依赖 | 完成后仍不代表 |
| --- | --- | --- | --- |
| P0 | QMT capability probe、已签名 capability manifest、route 字段证据、Python 3.6/`run_time`/callback/query 实测、named mapping/Interlocked ABI 证明 | Windows QMT 仿真环境、单受控 scope | 账户 `READY`、柜台最终受理或生产容量。 |
| P1 | wire codec、双 mTLS stream、证书/scope binding、Windows Edge Service、writer actor、完整 Edge 表与 health | P0 的部署事实；不依赖真实 mutation 结论 | QMT native caller 已接通。 |
| P2 | SPSC command/event ring、Native Engine client、Python 3.6 `QmtNativeEngine`、原生下单/撤单/查询 callback shim、production route codec | P0 manifest、P1 Edge | 恢复正确或性能达标。 |
| P3 | 完整查询终态、identity map、outbox replay、crash-boundary 对账、`BrokerRecoveryEvidence`、故障停单/受控恢复 | P0–P2 | 自动重报、自动撤单、`READY`。 |
| P4 | C0–C13 全链路遥测、性能压测器、原始时间对归档、单笔/100/300/max-safe burst、清理闭环 | P0–P3 全部通过 | EMT 的 SLA、微秒承诺或跨账户容量外推。 |

任何包未满足其验收门禁时，QMT production profile 固定为 `CAPABILITY_UNAVAILABLE`，scope 保持 `DISARMED`。不得通过遗留 `Helper + Gateway + Coordinator`、文件队列、轮询扫描或另一个 QMT 模型作为自动回退。

### 1.4 2026-09-30 Linux 侧代码复核与后续实施顺序

本次以 `tradecore-python` 当前代码复核 Linux 侧。结论为 **命令桥接已有实现，事件与恢复闭环尚未完成**；本次没有新增真实 Windows/QMT 探针证据，也不改变前述 P0 状态。

本次补齐的 Linux 边界：

- `QmtBrokerAdapter` 对已开始投递的 delivery 序列检查连续性；同一 delivery ID 只允许同一事实重投，拒绝倒退、跳号、内容替换与越过 ACK 失败的记录。observer 成功后才记住事实并 ACK；ACK 失败重投不重复投影，observer 抛错则不 ACK，保留重投机会。
- callback gap 先校验账户，再锁存 mutation stop；事件冲突、observer 异常、ACK 异常和提交/撤单的 bridge 异常也锁存停止交易。普通 `stop/start` 不清除锁存、游标和去重记录；查询/恢复入口保留。该锁存是当前对象的内存状态，不是跨进程恢复证据，也不提供自动解锁。
- `QmtMtlsBridgePort` 在 HELLO 失败时释放已创建连接；不健康或非法 health、错误关联、非法 sequence/time/state、`UNKNOWN_EFFECT` 和拒绝响应均关闭现有通道，不自动重发。撤单检查 venue、instance、scope；错误信息不复制对端任意拒绝载荷。

仍需按以下顺序实施：

| 顺序 | Linux 交付物 | 当前缺口与完成条件 |
| --- | --- | --- |
| L1 | mTLS event stream、delivery codec、ACK/RESUME | `QmtMtlsBridgePort.acknowledge()` 仍显式报未实现；`open()` 只有 COMMAND HELLO。需与 Windows outbox 共同冻结 event/gap/health envelope、scope/generation、delivery hash、resume cursor，打通独立事件连接。Windows endpoint 当前还拒绝 ACK，不能只改 Linux 即宣称连通。 |
| L2 | Journal-first callback ingress 与持久游标 | 当前依赖 observer 成功返回，尚无 QMT 专用运行装配证明 mmap commit 完成。需绑定 durable observer、保存和恢复游标/去重、验证进程退出后重投。当前首次 delivery ID 由调用方起点决定，尚无经认证的 resume 起点，不能宣称首条之前无缺口。 |
| L3 | 完整查询与 `bridge_recovery.py` | 查询只有命令和 handle；没有查询行/明确 QUERY_COMPLETE 的网络接入。`collect_recovery_evidence()` 仍显式报未实现。需有界聚合、超时/末页/错误判定、identity 分类、outbox replay、证据持久化；完成上限为 `RECOVERED_DISARMED`。 |
| L4 | 心跳、断线、背压和 EMS stop gate | 本次只补本地已观测错误的停止交易锁存；仍需事件连接故障主动传播、心跳超时、有界队列、恢复协调与 H0–H4 证据。不能用内存锁存替代整个账户的 EMS 停单和重启门禁。 |
| L5 | production profile、运行装配、Linux 部署 | `registry_declaration()` 仍生成 fake adapter。需 production mapping/capability/route evidence、真实 bridge factory、账户绑定、配置/证书生命周期、systemd 和预检。P0 证据缺失时继续 `CAPABILITY_UNAVAILABLE`。 |
| L6 | C0–C13 / R0–R8 / H0–H4 与集中验收 | 需真实 Linux–Windows mTLS、断线与 crash-boundary 演练、端到端 callback/query/recovery 和原始计时归档。离线测试不能作为跨机性能或 QMT 交易验收。 |

验证范围：Windows 开发环境中的无 SDK、无网络 QMT 合同/故障测试及静态检查；未部署 Linux 服务、未操作 QMT 账户。下一实施单元是 L1 事件协议及双端离线联调，再接入 L2 的实际 Journal observer。

### 1.5 2026-09-30 同级 Adapter 门禁与事件连接增量

在 1.4 的复核之后继续实施，当前强制层次如下：

```text
TradeCore OMS / EMS / VenueAdapterRegistry
                  │
          BrokerAdapter / BrokerEventObserver
                  ├── EmtBrokerAdapter → EMT SDK/session
                  └── QmtBrokerAdapter → QmtBridgePort
                                           ├── mTLS COMMAND
                                           └── mTLS EVENT / ACK / resume
                                                     → Windows Edge → QMT Native Engine
```

- EMT 与 QMT 是同级实现，不互相继承、导入或转发；mTLS、Windows Edge、ring 和 native policy 均是 QMT 内部细节。OMS、EMS、Consumer 和通用 Adapter 包只能依赖通用合同。
- `tradecore-import-policy.v1.json` / `check_tradecore_boundary.py` 新增 `TCB104`（通用层不得引用具体 venue Adapter）、`TCB105`（同级 Adapter 不得互相依赖），并将 EMS 纳入通用层检查。测试同时覆盖绝对和相对 import。
- `QmtAdapterProfile.bridge_registry_declaration()` 新增真实 bridge 的 DISARMED 注册入口；同一个 `VenueAdapterRegistry` 已测试可装配 `EmtBrokerAdapter` 与 `QmtBrokerAdapter`。原 fake 工厂保留作夹具。构造阶段不连接网络，不提供恢复完整性证书，不授权 mutation。
- `QmtBridgeError` 归属通用 `BrokerAdapterError`，网络传输错误不会要求 EMS 依赖 QMT 的具体异常类。

本轮连接实现：

| 文件 / 能力 | 已完成的代码边界 |
| --- | --- |
| `event_wire.py` | `QmtEventHelloV1`、`QmtEventHelloAcceptedV1`、`QmtEventDeliveryV1`、`QmtEventAckV1`、`QmtEventHealthV1`；严格字段、枚举、账户、scope 与 outbox epoch 校验；只接受标准化 event/gap，不接受 ring 原始字节。 |
| `event_client.py` / `mtls_bridge.py` | 独立事件 mTLS 连接、精确 resume HELLO、逐笔事件接收、单条 in-flight 背压、delivery ID + payload hash ACK 及回执。命令连接和事件连接沿用 TLS1.3 双向 CA + leaf pin 验证。没有事件连接时禁止 submit/cancel；不自动重发命令。 |
| `edge_event_endpoint.py` / `edge_store.py` | 在 SQLite 所属线程运行的事件会话：持久绑定 scope/account/epoch，有界读取下一条 outbox，校验 ACK 后提交。按 Linux resume cursor 重投保留记录，即使该记录的 ACK 已在 Edge 提交，也能处理 ACK 回复丢失。 |
| callback 故障传播 | 事件接收失败关闭两个连接，由 QMT Adapter 经通用 `on_callback_gap` 通知上游并锁存停止交易；正常事实仍走同一个 `BrokerEventObserver`。 |

`QmtMtlsBridgeConfig.events` 可选配置包含 Linux 逻辑 `account_id`、标准化 outbox 的持久 `outbox_epoch`（SHA-256 格式标识）和 `resume_delivery_id`。默认 cursor 为 0；非零值必须由实际 Journal 恢复提供，不允许从内存估计或盲用 Edge 最大 ID。epoch 不是 TCP session 或 ring generation；更换数据库必须换 epoch 并重新对账。当前版本保留已 ACK outbox，尚不允许自动清理历史。

验证：121 项相关测试与 12 个子测试通过，涵盖公共 BrokerAdapter/Registry、EMT/QMT profile、EMS/Consumer runtime、导入边界与 QMT 连接。新增真实本机回环 socket 的 TLS1.3 双证书握手与事件/ACK，以及 SQLite、Adapter、通用 observer 的组合测试；覆盖 ACK 回复丢失、observer 异常、错误 epoch/scope、跨账户、跳号、错误 ACK、raw ring 拒绝和新客户端显式 resume。它们仍是 Windows 开发机上的连接合同测试，不是 Linux–Windows 跨机或 QMT 原生验收。

**仍未完成的装配：**现有 `QmtEdgeService` 生产入口仍是 command listener，尚未把新事件 endpoint 接入它的单 writer actor；Native Engine callback → 标准化 event outbox 的 projector 未完成。Linux connection owner 需在专用事件 worker 调用 `receive_one_event()`，当前不会隐式创建后台线程。真实 mmap Journal observer/cursor 持久化、完整查询/恢复、生产配置/systemd、证书生命周期与跨机计时仍待实施。不能把本节的 endpoint/客户端测试写成生产服务已支持双通道。

本节更新 1.4 的 L1/L4/L5 状态：事件协议与客户端、事件会话以及 DISARMED 注册入口已有代码；剩余重点转为双端运行装配、标准化 callback projector 和 L2 Journal-first 持久化闭环。P0 与真实交易门禁保持原结论。

### 1.6 2026-09-30 提交与推送补记

本轮此前仅留在开发机的 QMT 改动已完成提交与远端推送：

| 项目 | 实际结果 |
| --- | --- |
| 分支 | `dev` → `origin/dev` |
| 代码提交 | `c12ee283487f63b90413b5e17a6e754ebeeec3a4`，`feat(qmt): enforce peer adapter boundaries and add resumable event transport` |
| 推送范围 | `5a6ad4b..c12ee28`，包含 §1.4/§1.5 的 Adapter 安全边界、同级依赖门禁、bridge 注册入口、事件协议/客户端/Edge endpoint、测试和实施文档。 |
| 远端核验 | 推送后 `git ls-remote origin refs/heads/dev` 与本地代码提交 SHA 一致。 |
| 提交前复验 | 121 项测试、12 个子测试通过；QMT 包 Pyright 为 0 errors / 0 warnings；暂存 diff 检查通过。 |
| 后续工作约束 | 已写入仓库 `AGENTS.md`：每次完成修改后提交并 push 到对应分支，核验远端并报告 commit。 |

Windows 从零部署可从远端取得该代码版本，先前交接中的“最新事件连接代码尚未提交/推送”说明已失效。拉取时记录实际 HEAD，并验证包含 `c12ee28`；源码可取得不等于 Windows 服务装配或真实 QMT 验收完成，剩余范围仍以 §1.5 为准。本补记通过后续文档提交同步推送。

### 1.7 2026-10-08 收录 Windows 本机 DISARMED 初始化帧验收

证据来源为用户转述的 Windows 部署 agent 验收摘要；本地已核对关联代码提交，未在本轮重新运行服务器/QMT，也未取得完整原始日志或完整 DLL/frame digest。以下截断哈希仅作报告标识，不能替代可复验的完整 digest。收录日期不是推定的实际测试日期。

| 项目 | 部署侧报告结果 |
| --- | --- |
| 部署基线 | `D:\projects\TradeCore\qmt-edge`，源码 `src`，`dev`，起始 base `5a6ad4b8`；随后修复提交 `872c9ad`、`e839a3b` 已推送并被本地 `dev` 包含。起始 base 不等于最终受测 HEAD，后续归档仍需记录确切受测提交。 |
| P0 预检 | `PROBE_READY_DISARMED`、`mode=PROBE`、`mutation_allowed=false`、`native_policy=null`。独立 CPython `3.10.11` x64，SQLite `3.53.4` 通过仓库 WAL 修复门禁。 |
| 原子 DLL | `tradecore_qmt_atomic.dll`，报告 SHA-256 `3d2ed089…d4f9ff`；三个导出符号验证可用。此为本轮构建标识，不覆盖 §1.2 的历史 DLL 记录。 |
| QMT 运行环境 | 本机「迅投极速交易终端睿智融科版」，内置 Python `3.6.8`；经读盘 loader 加载引擎。 |
| 初始化帧 | `init()` 写入 event ring，monitor 输出 `NATIVE_EVENT_DURABLE` 和 `MONITOR_COMPLETE`；持久化帧确认为 `kind=31 (HEALTH)`、`payload=b"PROBE_INIT"`，非 `INIT_*` 故障帧；报告 payload hash 前缀 `c8f7ed3d…`。 |
| 实测范围 | 对齐修复后的跨进程 mmap 可见性和初始化健康帧落盘。无任何下单、撤单或查询副作用，未开放网络监听。 |

**缺陷与已合入修复：**原 header `<8sHH32s16sIIQQQQ` 的四个 64 位计数器偏移为 `68/76/84/92`，不满足 Windows Interlocked 的 8 字节对齐要求。部署侧发现引擎对齐检查拒绝了指针，`init()` 未产出健康帧；Edge 预检未检查对齐，先前预检成功不足以证明此链路可用。`872c9ad` 在共享合同、Windows ring 和内嵌引擎中统一加入 `4x` 填充，将偏移改为 `72/80/88/96`，并更新引擎哈希。`e839a3b` 增加 `bigqmt_strategy/qmt_strategy_loader.py`，用于在 QMT 内从磁盘加载 P0 策略。两项修复作者均为冯宇。

**本地复核与剩余问题：**当前 `dev` 代码基线 `e839a3b028227dd538e5d307af37d13fc6e9b6a4` 与远端一致，121 项测试、12 个子测试通过；本轮仅补录文档。健康帧成功不消除以下缺口：

1. header 布局改变后 `RING_ABI_VERSION` / 内嵌 `ABI` 仍为 `1`，旧 ring 可能被新代码接受并错误解释计数器；需增加明确的 ABI 区分、旧布局拒绝测试和受控迁移流程。
2. pinned engine hash 当前绑定 CRLF 字节，而 Git 中保存 LF，仓库尚未固定该文件 checkout 换行规则；需固定字节规则并同步 pin，保证从零部署可复现。
3. `PROBE_INIT` 仅证明初始化健康帧；timer 持续运行、订单回调、查询完整性、route/policy 和签名 capability manifest 仍属后续 P0 工作。
4. `event_wire.py`、`event_client.py`、`edge_event_endpoint.py` 与 `bridge_registry_declaration()` 已存在，但双通道生产装配尚未实施；仍需 §1.5 所列单 writer、callback projector、Linux Journal/cursor 和跨机 mTLS 验收。

结论更新为 **Windows 本机 DISARMED 初始化链路通过，P0 capability 未整体通过，Linux–Windows 双通道未验收**。EMT/QMT 同级 Adapter 边界保持不变。下一步先处理 ABI 迁移与哈希可复现性，再开展持续探针及双端连接装配。

### 1.8 2026-10-08 ABI 升级与持续生命周期探针交付

本节落实 §1.7 的部署前置修复及下一轮无交易副作用探针；不将离线测试或上一轮 ABI-1 健康帧证据作为本轮真实 QMT 验收。EMT 与 QMT 继续通过同一通用 BrokerAdapter 合同同级注册，无相互依赖。

| 交付项 | 行为与验收边界 |
| --- | --- |
| 明确 ABI-2 | Edge 和 Python 3.6 内嵌引擎统一使用 revision `2`，拒绝未对齐和已对齐两种 ABI-1 header；`QmtRingHeaderV1` 仅保留 Python 值对象 API 名称，wire revision 为 2。旧 activation ABI digest 不再匹配，不能沿用签名 manifest。 |
| 原子对齐与文件保护 | Edge 同时检查实际地址和 DLL 的 alignment 导出，Interlocked 前拒绝未对齐指针；ring 创建使用独占方式，不能覆盖旧证据，打开失败释放 mapping。 |
| 受控迁移 | `prepare_probe_ring_upgrade.py` 只生成新配置、新 generation 和新 ring 路径，保留旧 ring、活动配置和 SQLite；只允许无 command history 的 PROBE 数据库，有历史命令则拒绝并等待恢复方案。部署侧按 README 先停 QMT 再停 Edge，审阅并安装配置后预检，不能原地改 ABI 字节。 |
| 引擎字节可复现 | `.gitattributes` 固定 `qmt_native_engine.py` 为 LF，并同步 loader 的 SHA-256；真实 QMT 使用 `qmt_strategy_loader.py` 读盘入口，避免编辑器中旧的 inner loader pin。 |
| 持久化后释放事件 | Edge 先 peek/校验，再提交 SQLite outbox，最后更新 ring consumer。写入失败保留帧；提交后释放前中断可幂等重投；原始 event ID 包含 generation，保留数据库时新的 sequence 1 不会冲突。SQLite `NORMAL` 的断电语义未改变，本轮不宣称主机断电验收。 |
| 持续生命周期证据 | PROBE 每次成功 timer drain 发出 tick/QPC 健康帧，stop 发出停止标记并释放 mapping；重启 tick 从 1 开始。无任何下单、撤单、查询或 listener。 |
| 严格 monitor | 仅空 ring 可以等待；无帧、init-only、CRC/存储失败、fault/non-health、sequence/tick 缺口、timer 超时、缺失 stop 或 stop 后仍有帧均失败。`--expect-stop` 还要求至少 1 秒静默窗口。单窗口最多 100,000 个 timer 样本。 |

离线验证：150 项测试、12 个子测试通过；QMT 包 Pyright 0 errors / 0 warnings，变更 Python 文件 Ruff 通过。新增覆盖两种旧布局、跨 Edge/内嵌引擎的对齐与 ring wrap、禁止覆盖、配置迁移、SQLite 失败/提交后重试、generation 去冲突、真实内嵌函数的仿真生命周期以及 monitor CLI 失败退出。内嵌代码仍可按 Python 3.6 语法解析；暂存版本的引擎与 loader 在 `core.autocrlf=true/false` 两种全新 checkout 下均通过 LF 与 SHA-256 核验。以上内嵌执行使用测试原子实现和模拟 scheduler，不替代真实 DLL/QMT 验收。

部署测试入口与顺序已写入 [Windows README](../../deploy/windows-qmt-edge/README.md#deployment-test-handoff-init-sustained-timer-stop-and-restart)：拉取 `dev` → 停两端并保留证据 → ABI-2 配置迁移 → 预检 → 先 monitor 后 QMT loader → 连续 timer/stop 静默 → 同 generation 再启动复验；归档确切 HEAD、完整 hash、版本和两轮完整脱敏 JSONL，提交推送验收摘要。

验收上限为 `PROBE_INIT_TIMER_STOP`。当前仅统计实际 callback 间隔的 P50/P95/P99/P100，官方 scheduled 时间未取得，显式保存 `scheduled_ns=null`；不得称为 scheduler jitter 或最小周期验证。route/callback/query 的事实与签名 manifest、双通道生产装配、Journal cursor、恢复与性能仍按 §4–§8 后续实施。

### 1.9 2026-10-08 ABI-2 本机部署与两轮生命周期验收（实际结果）

本轮按 §1.8 与 Windows README 完成 ABI-2 迁移，并在真实 QMT 上完成两轮 init / 持续 timer / 停止静默 / restart 探针。结果以本机实测为准，脱敏 JSONL 已随本提交归档于 `artifacts/qmt-edge-p0-abi2/`。

| 项目 | 实测结果 |
| --- | --- |
| 受测 HEAD | `e420a9b65468252de6f4139a9c09c6b13ad5aa8f`（`dev`，与远端一致） |
| 引擎 SHA-256 | `5e80b6f5d4ddf137388ea0d1df96a4d387296fa5be9dfb883079b7b4a5ccd9c7`（LF，`.gitattributes` `eol=lf` 生效） |
| 原子 DLL SHA-256 | `3d2ed089544f81d5e166bf361cd9d89d6e0499f7ee17ecd400f6bc7736d4f9ff` |
| native_abi_digest | `71e420c9cf759724e5482febc47924f3946d46f9fd814986459b4ca6c4ce5cc3` |
| ring_abi / generation | `2` / `2ef0e65936984b0793faad4bef0f4700` |
| Edge / SQLite | 独立 CPython `3.10.11` x64 / `3.53.4`（WAL 修复门禁通过） |
| QMT 运行环境 | 本机「迅投极速交易终端睿智融科版」，内置 Python `3.6.8`，经 `qmt_strategy_loader.py` 读盘加载 |

| 轮次 | 结果 |
| --- | --- |
| 第一轮 | `acceptance=PROBE_INIT_TIMER_STOP`，`timer_samples=51`，事件 53（PROBE_INIT + 51×PROBE_TIMER + MODEL_STOP）；timer 间隔 P50≈999.7ms / P95≈1012ms / P99≈1013.7ms / P100≈1013.7ms；`scheduled_ns=null` |
| 第二轮（restart） | `acceptance=PROBE_INIT_TIMER_STOP`，`timer_samples=65`，事件 67；同 generation/ring/DB 续接、tick 从 1 重起；间隔 P50≈999.95ms |

| 归档日志 | SHA-256 |
| --- | --- |
| `artifacts/qmt-edge-p0-abi2/round1-monitor.jsonl` | `36e4bacd61c173e971cc5c8a71996abf6a0b8ad6d78deee8433f1c32fbf15a40` |
| `artifacts/qmt-edge-p0-abi2/round2-monitor.jsonl` | `4ab4fc7e5d60f025b83aeb5c8c8999f90bf41fdcd3ef1d40830b11aa44fe06a0` |

迁移流程：保留旧 ABI-1 ring/config 证据 → `prepare_probe_ring_upgrade.py` 生成新 config/generation/ring 路径 → 安装到活动 config → 预检 `PROBE_READY_DISARMED` + `ring_abi=2` → 先 monitor 后 QMT loader，两轮完成。

结论：**ABI-2 本机 DISARMED 生命周期（init + 持续 timer + 停止静默 + restart）两轮通过，验收上限 `PROBE_INIT_TIMER_STOP`**。此结果不代表 scheduler jitter、最小周期、订单回调、查询完整性、route/policy、签名 capability manifest、跨机双 mTLS 或断电耐久验收；相关能力仍按 §4–§8 后续实施。本轮无下单/撤单/查询副作用，未开放网络监听。

### 1.10 2026-10-09 双通道 DISARMED 传输探针交付

本轮基于 `540bd0a` 完成传输联调入口，固定专用链路：Windows `192.168.251.2:19443`，Linux 客户端源地址 `192.168.251.1`；COMMAND/EVENT 为同一端口上的两条独立 mTLS 连接。公网不在本轮配置内。Windows 侧此前的 ping/SSH 证据只证明它到 Linux 可达，反向新端口仍待启动后的实测。

交付与修复：

- `QmtEdgeService` 改为有界连接数/队列、单存储 actor，SQLite runtime 在 owner 线程内创建/使用/关闭，修复原连接线程访问主线程 SQLite 的运行装配问题；停止时先中断并回收 socket worker，再关闭存储。旧原生命令服务保留独立 owner，原始 ring outbox 不直接出网。
- 新增 `transport_probe.py` 与 `probe_deployment.py`，Windows 以专用 FULL outbox 发送固定测试序列，Linux receipt journal durable 后 ACK，收到精确 ACK 回执后才提交 cursor；两端状态均有排他进程租约及专用数据库标识。重启从相同身份继续，重复帧必须逐字节一致。
- 测试使用 `QmtTransportProbeDeliveryV1` 独立 schema，生产 broker event 解码器拒绝；COMMAND 一律 `TRANSPORT_PROBE_DISARMED`，不构造原生请求。`capability_accepted=false`、`mutation_allowed=false`，不打开 QMT、DLL、ring 或账户。
- `tools/qmt_bridge_pki.py` 提供短期测试 CA、各端本地私钥/CSR、签发与 leaf DER SHA-256 检查；仅交换 CSR/公有证书，不自动分发私钥、开防火墙或启动远端监听。

开发侧验证：Windows 与 Ubuntu WSL 均为 **132 passed**（`tests/test_qmt*.py`），其中新增 21 项包含真实 TLS 双连接、两类持久化/ACK 中断后重启续传、错误 CA/pin/hostname/scope/epoch、journal 失败不 ACK、单事件 owner、并发连接上限、数据库身份/租约、生产 schema 拒绝和客户端 CLI。QMT 包 Pyright 为 0 errors / 0 warnings；变更 Python 文件 Ruff 通过。仅使用本机回环证书与临时测试数据库，不代表 `.1 ↔ .2` 已联通。

执行步骤、两端配置、证书交接及退出码见 [双通道部署手册](../../deploy/windows-qmt-edge/DUAL_MTLS_PROBE.md)。本轮验收上限为 `DUAL_MTLS_TRANSPORT_PROBE_DISARMED`，**实际跨机验收仍待部署侧执行**。Linux receipt journal 是探针 SQLite 日志，不能称作生产 OMS mmap Journal observer 已完成；原生 callback projector、生产双端运行装配、查询完整终态、P0 manifest、P3 恢复与 P4 性能仍保持原门禁。EMT/QMT 继续是通用 BrokerAdapter 的同级实现。

### 1.11 2026-10-09 Linux 签发与客户端部署准备

本对话承担开发及 Linux 部署职责；已复用项目现有受控部署凭据实际登录 `xuanling@192.168.251.1:22`，本机连接简称配置为 `tradecore-linux`，别名中不含凭据。Linux 接口清单确认 `192.168.251.1` 与 `60.190.249.91` 属于同一主机，专用接口实际配置为 `/24`（此前 Windows 报告为 `/30`）；未修改现有接口、路由或 Consumer 服务。

| 项目 | 实测/部署结果 |
| --- | --- |
| 隔离源码 | `/opt/tradecore-qmt-probe/releases/84ee97207c1f042c6a2843656c38237a9e3d4c8f`，从服务器 Git 对象导出，保持与 Windows 本轮源码基线一致 |
| 独立环境 | `/opt/tradecore-qmt-probe/.venv`，CPython `3.10.21`、SQLite `3.53.1`、cryptography `50.0.2`；安装后的探针源码 hash 与隔离 release 一致 |
| 运行身份 | 新建系统用户 `tradecore-qmt-probe`，nologin；certs/config/data/evidence 目录 `0700`，Linux leaf 私钥 `0600` |
| 签发身份 | CA 私钥由部署操作者独立持有，`ca-private` 目录 `0700`；已以运行用户验证不可读取 CA 私钥 |
| Windows CSR | 签名、CN、SECP256R1 校验通过；DER SHA-256 `9f560f968e7a8a0d296730a2115ae1ef7a19d21421c1d0a7c83597f0d3701a37` |
| Windows leaf | 已签发，公钥与 CSR 一致，IP SAN `192.168.251.2`；DER SHA-256 `2093317f792651ed1776353853d222d917f87c7e7d23e820237ac50be5e5a4fd` |
| Linux leaf | 私钥仅在 Linux 本机生成/保存，IP SAN `192.168.251.1`；DER SHA-256 `709621bc077d26625945d70d68d8a5cc1b505bd598c01c464011f9294aab3226` |
| 测试 CA | DER SHA-256 `39a8262700e34851bd88ddc18a623fdf3de7ea1297136fe95129c5203dfe6683`；CA 到期 `2026-11-08T06:39:22Z`，两端 leaf 到期 `2026-10-16T06:39:22Z` |
| 路由/连通性 | `192.168.251.1 → 192.168.251.2` 使用专用接口；本轮到 `.2:19443` 的有限 TCP 探测为 TimeoutError，不推定为 TLS 成功或确认无监听 |

Linux 活动配置位于 `/var/lib/tradecore-qmt-probe/config/transport-probe.json`，固定 DISARMED/8 帧；共同 scope/epoch 已生成并写入双方配置，保留于受控交接 metadata。私有预检证据为 `/var/lib/tradecore-qmt-probe/evidence/preflight-20261009.json`。

公有交接包在服务器 `/var/lib/tradecore-qmt-probe/handoff/qmt-probe-windows-public-20261009.zip`，已取回本地；仅含 CA/双端 leaf 公有证书、Windows 配置、metadata 和说明，没有任何私钥。包 SHA-256 为 `684f0e31284f6e3b5f909ce688abd8cb4a01fcea05451f4f31d39a81a74fccb0`。用户随后明确授权公有包入库，现保存为 `artifacts/qmt-bridge-handoff/20261009/qmt-probe-windows-public-20261009.zip`，供 Windows agent 直接拉取 `dev`；CA/leaf 私钥和其余私有运行文件仍不提交 Git。

当前结论：`LINUX_SIGNING_AND_CLIENT_PREPARED`；**`PENDING_WINDOWS_LISTENER`，未执行 COMMAND/EVENT 跨机握手、8 帧完成、中断/restart 或负面矩阵**。Windows 应先安装公有证书和已填配置、核对本机私钥匹配与指纹、按 §1.10 手册启动专用监听，再由本对话执行 Linux 侧联调。未启动 QMT 模型，未调用查询/下单/撤单，未签发 capability。

### 1.12 2026-10-09 首轮真实跨机传输与负面检查

Windows agent 回报受测源码 `ecb6530900c7b2f73c8c25d2c860969890d08f9c`，已监听专用 `.2:19443`，但新增防火墙规则因非管理员被拒。Linux 实测从 `.1` 到该端口的 TCP 和双向 TLS 均成功，故“没有新增规则就一定连不上”的判断不成立；成功连接不证明现有 Windows 入站 ACL 已限制到 `.1`，该范围仍待部署侧审计，不据此关闭防火墙或扩大规则。

Linux 继续使用独立 release `84ee972`。本地 Git 比较确认 `84ee972..ecb6530` 的 adapters、domain 与 PKI 工具无代码差异；两端完整仓库 HEAD 不相同，未将其表述为同一完整 revision。Linux 实际 probe module SHA-256 为 `1b28eb12d5e85afa7a0c21d265b1c28da10ec395cbbdbe14a886b8a99f9c9b1f`。

| 检查 | Linux 实测结果 |
| --- | --- |
| 首次 COMMAND/EVENT 双通道 | 退出码 0，`TRANSPORT_PROBE_COMPLETE`，received=8、replayed=0、confirmed_cursor=8；receipt 表 8 行 |
| 完成游标续接 | 退出码 0，received=0、confirmed_cursor=8，未重复接收 |
| 错误 server leaf pin | 退出码 2，`TRANSPORT_PROBE_BLOCKED`；独立 journal 的 receipt/cursor 均为 0 |
| 错误 scope | 退出码 2，`TRANSPORT_PROBE_BLOCKED`；独立 journal 的 receipt/cursor 均为 0 |
| 错误 epoch | 退出码 2，`TRANSPORT_PROBE_BLOCKED`；独立 journal 的 receipt/cursor 均为 0 |
| A 轮：第二帧 durable 后、ACK 前中断 | 退出码 3，`INTERRUPTED_AFTER_JOURNAL`；receipt=2、confirmed_cursor=1，等待 Windows 进程重启后再续传 |

脱敏原始结果和 receipt/cursor 核对已归档为 `artifacts/qmt-bridge-acceptance/20261009/phase1-linux.json`，文件 SHA-256 `e958b14b9ab4cb67ce9a9e29056d7356a9d5d6a3726738bc24c26b09354b0d75`。所有实际运行均 `probe_only=true`、`mutation_allowed=false`、`capability_accepted=false`，未运行 QMT 模型或原生查询/交易。

当前结论：**基础跨机双通道与三项负面检查通过，完整验收仍为 `PARTIAL_PENDING_WINDOWS_RESTART_AND_ACL_AUDIT`**。尚缺 A 轮 Windows 进程重启后的精确重投、B 轮 ACK 后/cursor 前中断及 Windows 重启恢复、Windows 入站 ACL 范围核对；没有提前签发 `DUAL_MTLS_TRANSPORT_PROBE_DISARMED` 完成结论。

A 轮待续配置为 `/var/lib/tradecore-qmt-probe/config/live-A-20261009T070759Z.json`。Windows 侧须只停止并重启同一探针，保留原 outbox/配置/scope/epoch，回报新 PID 与 `TRANSPORT_PROBE_LISTENING` 后由 Linux 侧去掉故障参数续传；不要删除 outbox 或 Linux journal 来制造成功结果。

### 1.13 2026-10-09 A 轮重启续传通过，B 轮已中断待重启

Windows 部署 agent 经用户回报旧 PID `28556` 已退出、新 PID `22264` 已监听，保留 outbox/WAL、配置、scope 与 epoch。PID 证据来自转述，Linux 未独立检查 Windows 进程。Linux 继续使用 §1.11 的隔离 release 和运行身份，以 A 轮原 journal 续传。

| 证据 | 实测结果 |
| --- | --- |
| A 轮实际时间 | `2026-10-09T09:34:16.620315+00:00` |
| COMMAND 实际 socket | `192.168.251.1:45245 → 192.168.251.2:19443` |
| EVENT 实际 socket | `192.168.251.1:40313 → 192.168.251.2:19443` |
| 两条 TLS | `TLSv1.3 / TLS_AES_256_GCM_SHA384`；peer leaf SHA-256 均为 `2093317f792651ed1776353853d222d917f87c7e7d23e820237ac50be5e5a4fd` |
| A 轮续传 | `TRANSPORT_PROBE_COMPLETE`，received=7、replayed=1、confirmed_cursor=8；receipt 为 1–8 共 8 个唯一序号 |
| B 轮中断 | `2026-10-09T09:35:37.826429+00:00`，退出码 3，`INTERRUPTED_AFTER_ACK_BEFORE_CURSOR`，received=2、receipt=2、confirmed_cursor=1 |

实际连接证明本轮使用专用直连目标；Linux 侧未修改 Windows 防火墙，也未使用公网目标。仅按端口查不到 19443 规则、默认入站 Block，不能解释完整有效策略：仍须检查当前 profile 是否启用、接口排除项、Any 端口的程序/服务规则及策略执行状态。**可连接不等于入站范围已经符合要求**，入站 ACL 验收保持待定。

新增只读脚本 `deploy/windows-qmt-edge/scripts/inspect-probe-firewall.ps1` 收集上述候选规则与监听 PID。非管理员或查询错误明确输出 `PARTIAL_REQUIRES_ADMIN_REVIEW`，不得将空列表视作不存在规则；脚本不创建规则、不修改服务或 profile。开发机完成 PowerShell 语法解析及非管理员失败路径实跑，正确收录权限/接口/监听查询失败；此测试不替代 Windows 部署节点的策略检查。

| 原始归档 | 文件 SHA-256 |
| --- | --- |
| `artifacts/qmt-bridge-acceptance/20261009/restart-A-linux.json` | `2a4af5e20f4c59383be233e3941c2a58923a5b217c7a0db9009fec2205c4f6b8` |
| `artifacts/qmt-bridge-acceptance/20261009/interrupt-B-linux.json` | `f75c6c8ed80600270aae5ff9a3ce19945f241dbba37d0834c3dac03a5e0c80cc` |

B 轮待续配置为 `/var/lib/tradecore-qmt-probe/config/live-B-20261009T093537Z.json`。必须在本次 B 中断后再停止并重启 Windows 探针，保留同一 outbox/配置/scope/epoch，回报新 PID 与 LISTENING 后才续传。预期 received=7、replayed=1、confirmed_cursor=8，最终仅 8 个唯一 receipt。

当前结论更新为 **A_RESTART_RESUME_PASS；完整验收仍待 B 轮 Windows 重启续传及 ACL 审核**。所有帧仍为 probe schema，mutation/capability 均为 false，未运行 QMT 模型、查询或交易，未改变生产验收门禁。

### 1.14 2026-10-09 B 轮重启续传通过，入站 ACL 待收敛复验

Windows 部署 agent 经用户回报旧 PID `22264` 已停止，新 PID `20676` 在 `192.168.251.2:19443` 重新监听，沿用 outbox/WAL、配置、scope、epoch。该 Windows 进程状态为转述证据；Linux 使用原 B 轮 journal 实际续传，并直接核对 socket、TLS 和 SQLite 内容。

| 证据 | Linux 实测结果 |
| --- | --- |
| 实测时间 | `2026-10-09T10:34:05.155757+00:00` |
| 受测 Linux release | `84ee97207c1f042c6a2843656c38237a9e3d4c8f`，隔离运行身份 `tradecore-qmt-probe`（uid 977） |
| 续传前 | receipt 序号 `[1, 2]`，confirmed_cursor=1 |
| COMMAND 实际 socket | `192.168.251.1:49255 → 192.168.251.2:19443` |
| EVENT 实际 socket | `192.168.251.1:54393 → 192.168.251.2:19443` |
| 两条 TLS | `TLSv1.3 / TLS_AES_256_GCM_SHA384`；Windows leaf 指纹均匹配 §1.11 的 `2093317f…5a4fd` |
| B 轮结果 | 退出码 0，`TRANSPORT_PROBE_COMPLETE`，received=7、replayed=1、confirmed_cursor=8 |
| 续传后 | receipt 序号恰为 `[1,2,3,4,5,6,7,8]`，唯一记录 8 条、confirmed_cursor=8 |

原始 Linux 证据为 `artifacts/qmt-bridge-acceptance/20261009/restart-B-linux.json`，文件 SHA-256 `5ad0a681f1ec15d5fd939eb0f5e732be8e8892ff97330f0c7a0734729839051b`。Linux 本轮未修改 Windows 防火墙；probe_only=true、mutation_allowed=false、capability_accepted=false，不涉及 QMT 模型、查询、下单或撤单。

**Windows 防火墙报告（用户转述，尚未取得完整 JSON）：**管理员巡检 `elevated=true / COLLECTED_REQUIRES_REVIEW / query_errors=[]`，三个 profile 均启用、默认入站 Block，发现 53 条候选程序规则。其中 Python 3.10 和 3.12 的 Public TCP Allow 规则 local/remote address 与 local port 均为 Any。该报告提供了此前可连接的候选放行来源，也表明尚不能认定端口仅向 `.1` 开放。需结合完整规则 Name、当前接口 profile、实际探针进程可执行路径核对匹配；不能仅凭程序显示名或候选数量断言每条规则均生效，也不能把 TCP 可达等同于未认证者可执行探针/交易。

下一步由 Windows 管理员保存现有规则及回滚信息，新增 `.2:19443/TCP ← .1` 的精确 Allow 规则；仅对确认适用于探针且超出本轮范围的宽泛规则按精确 Name 收敛，保留其他服务所需访问。不要按 `python.exe` 显示名批量删除规则，也不要因为存在 Python 3.12 候选就直接修改无关运行时规则。新增窄规则本身不会取消既有宽规则。随后再次运行只读巡检，归档脱敏的完整 JSON、实际进程路径/接口 profile 以及变更前后规则；由 Linux 使用已经完成的原 journal 做零新增帧续接复验，预期 received=0、confirmed_cursor=8。其他来源拒绝结果如未实际测得，应明确未测，不能以 `.1` 连通代替。

当前结论：**基础双通道、三项负面检查及 A/B 中断重启恢复均已通过；完整传输部署验收仍为 `PARTIAL_PENDING_ACL_HARDENING_AND_RECHECK`。** 不重跑或覆盖 A/B 证据，不提前宣称生产 READY、P0 capability 或完整 QMT 交易能力通过。

## 2. 不可变工程边界

1. 每个 QMT 账户只有一套 Edge、一个 Edge SQLite writer、一个 command-ring producer、一个 event-ring consumer、一个 QMT Native Engine 和一个 QMT native caller。不得以 Python 线程池或多模型并发调用同一账户。
2. QMT 凭据、登录态、QMT API 对象与本机 token 只存在 Windows。Linux、trace、evidence、日志和 Git 中不得出现它们。
3. `passorder(..., quickTrade=2)` 返回只记录 `C8` 的本机调用返回；不是柜台受理、订单 callback、成交、撤单完成或恢复完成。
4. Edge SQLite 是 Windows 请求/事件的耐久起点；ring 是低延时本机交接，不能成为唯一事实源。崩溃边界无法证明原生调用状态时固定为 `UNKNOWN_EFFECT`，绝不换 ID 重发。
5. QMT 内置脚本只能在官方 `run_time` 或经 P0 证明的官方 callback 上执行原生函数。不得在网络线程、SQLite writer、辅助线程或行情 tick 上偷偷调用。
6. `ContextInfo` 不保存长期状态。QMT Python 3.6 Native Engine 的 cursor、限速、短期去重和计时状态只存在普通模块全局对象 `G`。
7. 时间采集、脱敏 correlation、gap、backpressure、unknown-effect 与清理结论是功能合同的一部分；没有它们的“成功”一律不通过验收。

## 3. 统一穿透时间坐标与原始样本合同

### 3.1 命令与 callback 的 `C0`–`C13`

每一笔 `SUBMIT`、`CANCEL`、`QUERY` 或 `RECOVERY_BEGIN` 都生成唯一的脱敏 `trace_id`。后续所有原生 callback、查询行、`QUERY_COMPLETE`、故障事件和 Linux delivery 都带回相同 `trace_id` 或其不可逆 digest。`C0`–`C8` 描述一次命令的向前穿透；`C9`–`C13` 描述该命令可关联的原生事实如何回到 OMS。

| 点 | 主机 / 时钟域 | 真实边界 | 采集者 | 不可误写为 |
| --- | --- | --- | --- | --- |
| C0 | Linux monotonic | 策略/消费端信号被 TradeCore 接纳 | 策略入口 | 已通过 OMS 或 EMS。 |
| C1 | Linux monotonic | EMS 账户、风险、scope、kill-switch 授权完成 | EMS | 已写本地事实。 |
| C2 | Linux monotonic | `PRE_DISPATCH` mmap Journal commit 完成 | execution lane | 已发送到 Windows。 |
| C3 | Linux monotonic | canonical bridge command 最后一个字节交给 mTLS writer | bridge sender | Edge 已收到。 |
| C4 | Windows monotonic | Edge SQLite request/idempotency/correlation 事务提交 | Edge writer actor | command ring 或 QMT 已调用。 |
| C5 | Windows monotonic | command frame body/CRC 完整、commit word 最后发布 | Edge ring producer | QMT 已开始 drain。 |
| C6 | Windows/QMT monotonic | 官方 `run_time` callback 开始读取该 command frame | Native Engine | 已进入原生下单函数。 |
| C7 | Windows/QMT monotonic | 固定参数已解码、紧邻原生交易函数调用之前 | Native Engine | 原生函数已返回。 |
| C8 | Windows/QMT monotonic | 原生交易函数同步返回或抛出异常 | Native Engine | 订单已被柜台受理或有 callback。 |
| C9 | Windows/QMT monotonic | 原生 order/trade/cancel/query callback 首行进入 Native Engine | callback shim | Edge/OMS 已持久化该事实。 |
| C10 | Windows/QMT monotonic | event frame commit word 最后发布 | Native Engine event producer | Edge SQLite 已写入。 |
| C11 | Windows monotonic | Edge SQLite event + Linux outbox 事务提交 | Edge event writer | Linux 已收到或已 ACK。 |
| C12 | Linux monotonic | Linux callback mmap Journal commit 完成 | `QmtBrokerAdapter` ingress | OMS 已投影。 |
| C13 | Linux monotonic | `BrokerEventObserver` / OMS projector 可见该事实 | observer/projector | 最终成交、最终撤单或账户 `READY`。 |

每份时间样本必须包含 `operation_kind`、`fact_kind`、`trace_digest`、`scope_digest`、`adapter_instance_digest`、时间点、对应时钟域、sequence、完整性和错误类别。不得保存账户、证券、价格、订单号、原生 token、IP 或密钥。

### 3.2 各业务链路如何使用时间点

| 链路 | 必须成对记录的穿透段 | 终态 / 完整性门禁 |
| --- | --- | --- |
| 下单 `SUBMIT` | `C0→C1→C2→C3→C4→C5→C6→C7→C8`；随后相关 order callback 的 `C9→C10→C11→C12→C13` | C8 仅为 native return。至少一个可关联的 order callback 才可报告 `C0→C13`；没有 callback 是缺口，不以 sleep 补齐。 |
| 撤单 `CANCEL` | 以该撤单 request 自己的 `C0→C8`；相关 cancel/order terminal callback 继续记录 `C9→C13` | `C13` 必须是原生回调投影的终态事实；Edge 接受或 C8 不等同撤单完成。 |
| 查询 `QUERY` | query command 同样记录 `C0→C8`；每个 query row 及 `QUERY_COMPLETE` 都走 `C9→C13` | 只有 P0 已证明的末页/`is_last`/completion event 才能完成。空数组、短暂静默、行数推测均不可完成。 |
| 恢复所触发的查询 | 每一个恢复 query 仍用自己的 `C0→C13`，并关联同一 `recovery_run_digest` | 只有全部 query complete、outbox replay 无 gap、分类无冲突后才可以产生 `R7`。 |
| 原生异常 / ring fault | 命令已到 C7/C8 时保留已知点；callback side 从 C9/C10 或故障观测点开始 | 不完整样本标为 `UNKNOWN_EFFECT` / gap，仍归档；不得从性能样本中静默删除。 |

同一订单的首个 callback、订单状态变化、成交、撤单和查询行可能各有独立 `C9→C13` 对。报告必须按 `fact_kind` 分开，不得把多个 callback 的百分位相加或把 C8 冒充 C13。

### 3.3 恢复与健康传播时间点

恢复、停单并非一笔订单的 native call，另设下列坐标；其底层每个 query 仍遵循 `C0`–`C13`。

| 点 | 主机 / 时钟域 | 恢复链路边界 |
| --- | --- | --- |
| R0 | Linux | `begin_recovery` 请求被接纳，scope 已保持 `DISARMED`。 |
| R1 | Linux | 已保存最后 durable delivery cursor / route ledger cutoff。 |
| R2 | Linux | event stream 重新挂接并提交 resume cursor。 |
| R3 | Windows | Edge 未 ACK outbox replay 的最后一条 delivery 已 durable 发出。 |
| R4 | Windows/QMT | 已证明的原生查询/快照扫描开始。 |
| R5 | Windows/QMT | 每个所需 query family 的明确 complete 事实已生成。 |
| R6 | Linux | own / foreign / unattributed / conflict 分类、双扫描稳定性和 gap 检查完成。 |
| R7 | Linux | `BrokerRecoveryEvidence` 已 mmap durable commit。 |
| R8 | Linux | 恢复 gate 输出 `RECOVERED_DISARMED` 或 `RECOVERY_BLOCKED`。 |

| 点 | 主机 / 时钟域 | 故障停单传播边界 |
| --- | --- | --- |
| H0 | 发现故障的主机 | ring CRC/sequence、native exception、callback gap、证书失效或 QMT health fault 被检测。 |
| H1 | Windows | Edge health 已 durable 写为 not-ready，拒绝新命令。 |
| H2 | Windows | fault/gap event 已进入 outbox。 |
| H3 | Linux | fault event 已在 callback mmap Journal commit。 |
| H4 | Linux | EMS/QMT scope 的 stop-new-order gate 已生效。 |

`H0→H4` 必须有原始样本并按错误类别统计。任何 H 点缺失、故障后仍接收新单或误把 fault 当作健康，都使对应故障验收失败。

### 3.4 时钟与统计规则

- Linux 和 Windows 分别使用单调纳秒时钟；QMT Python 3.6 使用同一 Windows 主机的单调高精度时钟。不得使用 wall clock 计算主机内时延。
- `C0→C3`、`C4→C8`、`C9→C11`、`C12→C13`、`R0→R2`、`R6→R8` 可直接由同一主机单调时钟相减。
- `C3→C4` 与 `C11→C12` 跨 Linux/Windows。原始两端时间戳必须始终保留；只有测试窗口拥有经记录的时钟偏差/误差证书时，才可报告校正后的跨主机单向估计，并同时报告误差上界。否则只报告命令/事件的端到端墙钟和各主机内分段，字段标为 `CROSS_HOST_UNPAIRED`。
- `C5→C6` 是 QMT 官方 scheduler 等待；`C6→C8` 是 Native Engine / 原生调用返回。它们必须分开报告。
- 每个 workload 保存所有有效原始时间对，输出 `count`、缺口数、负值/乱序数、P50/P95/P99/P100、整批墙钟、成功、明确背压、权威 venue throttle、unknown-effect、callback-gap 与清理计数。任何缺口仍留在分母和报告中。

建议的脱敏原始记录结构如下；它是归档 schema，不是跨进程自由格式协议：

```json
{
  "schema": "QmtTraceV1",
  "trace_digest": "sha256:...",
  "operation_kind": "SUBMIT",
  "fact_kind": "ORDER_CALLBACK",
  "scope_digest": "sha256:...",
  "clock_domains": {"linux": "monotonic_ns", "windows": "qpc_ns"},
  "points_ns": {"C0": 0, "C1": 0, "C2": 0, "C3": 0, "C4": 0, "C5": 0, "C6": 0, "C7": 0, "C8": 0, "C9": 0, "C10": 0, "C11": 0, "C12": 0, "C13": 0},
  "completeness": "CALLBACK_COMPLETE",
  "cross_host_status": "CROSS_HOST_UNPAIRED",
  "error_category": null
}
```

示例中的零值仅表示字段形状；真实 evidence 不得把缺失点写为零，必须显式 `null` 和 `MISSING_*` 原因。

## 4. P0：能力事实、激活门禁与 ABI 探针

### 4.1 设计与实现范围

P0 的任务是把供应商/运行时事实写成可验证的 `QmtCapabilityManifestV1`，不是直接开启下单。production profile 只能读取一份经过验证签名、未过期、scope 匹配、build 匹配的 manifest；没有 manifest 或任一 capability 未通过时，构造阶段即返回 `CAPABILITY_UNAVAILABLE`。

| 实现单元 | 设计责任 | 必须记录的时间链路 |
| --- | --- | --- |
| `qmt_capability_manifest.py` 与 loader | 严格 schema、QMT build/embedded Python build、scope、有效期、probe digest、route 字段事实、ring ABI digest、签名/撤销校验 | 记录 manifest 评估耗时，但不进入 C0–C13 热路径。 |
| `qmt_capability_probe.py` | 以单一受控 scope 执行版本、Python 3.6、`run_time`、`quickTrade=2`、callback/query/撤单、rate-limit/reconnect probe；只输出脱敏 evidence | 每个真实 command/callback 使用 C0–C13；timer probe 额外逐次记录 `scheduled→C6`。 |
| `native_engine_contract.py` | 固定 ABI revision、slot/header offset、CRC、payload bounds、scope/generation/role 校验与 manifest digest | 对每次 publish/read 记录 C4/C5/C6/C10；不做 SQL/JSON 计时替代。 |
| Windows Interlocked probe | 验证 mapping 打开、对齐、位宽、release/acquire、commit-last、wrap、crash 读取与拒绝行为 | command path 至少记录 C4→C6；event path记录 C9→C11；撕裂必须生成 H0。 |
| `route_token.py` production codec | 根据实测的字段容量、字符集、回显与 stable identity 生成无损 route/reference | 下单、查询、callback、恢复各至少一组关联 C0–C13；任何截断/冲突为 H0。 |

P0 后新增 `QmtCapabilityActivation`。`QmtBrokerAdapter` 不得再以裸 `mutation_enabled=True` 作为生产授权；只有 activation 同时校验 manifest、签名/撤销、scope ownership、adapter instance、Edge identity、证书有效期、ring ABI 和显式 `DISARMED` 运维状态后，才可使 production submit/cancel 路径具备“可尝试”的资格。

### 4.2 P0 验收

1. 在 QMT 仿真环境验证内置 Python 3.6、模型生命周期和官方 `run_time`。报告首次触发、最小可用周期、P50/P95/P99/P100 抖动、overrun、停止模型后的行为；不能以文档或历史 Gateway timer 代替。
2. 验证 `quickTrade=2` 只在官方允许上下文调用，普通全局 `G` 可保持状态，`ContextInfo` 不承担状态；native caller 数为一。
3. 验证命名 mapping；若失败，才验证启动时一次打开的固定 file-backed mapping。两者均必须验证 Interlocked、alignment、commit-last、CRC、wrap、full、producer/consumer crash 和 DACL。失败时不允许进入 P1/P2 激活。
4. 以受控、可清理的仿真订单验证 route 字段的容量/编码、order/trade/cancel/error callback 回显、query 回显与 stable native identity。未能无损关联则 production codec 不生成。
5. 每个 query family 必须有明确 completion 证据；空结果、静默或 SDK 返回不构成 completion。
6. probe 返回的原始时间对、能力结论和 error category 全部脱敏归档。只有上述所有检查成功才签发 manifest；否则签发失败 evidence 并保持 `CAPABILITY_UNAVAILABLE`。

## 5. P1：Linux–Windows 可信 bridge 与 Edge 持久化服务

### 5.1 设计与实现范围

P1 只建设可恢复、可认证、有界的跨主机和 Edge 耐久边界，不在 network/SQLite 线程调用任何 QMT 原生函数。

| 文件/单元 | 实现内容 | 主要穿透段 |
| --- | --- | --- |
| `adapters/qmt/bridge_protocol.py` | magic、长度、schema revision、kind、request/event ID、scope digest、payload hash 的 canonical binary envelope；拒绝未知/超长/重复字段 | Linux `C2→C3`，Windows `C3→C4`。 |
| `adapters/qmt/bridge_transport.py` | 两条独立 mTLS stream、resume cursor、心跳、有界 writer/reader、重连与 backpressure；不使用无界队列 | `C2→C4`、`C11→C12` 与 `H0→H4`。 |
| `deployment/windows-qmt-edge/edge_protocol.py` | Edge server 证书/peer scope/instance binding、command admission、event delivery/ACK | `C3→C4`、`C11→C12`。 |
| `edge_store.py` 扩展 | 单 writer actor、`requests/native_identity_map/events/outbox/ring_cursor/recovery_runs/health` 表、事务和 crash states | C4 及 C11 只能在 durable commit 后打点。 |
| `edge_service.py` / `edge_health.py` | 服务 composition、受限工作目录、health projection、stop-new-order；不含 OMS/EMS/QMT SDK object | H0→H2。 |
| service 安装清单 | 服务 SID、ring DACL、证书位置、只允许 Linux 对端的防火墙规则、rotation/revocation 检查 | 部署检查的 H0→H4，非下单热路径。 |

`SUBMIT/CANCEL/QUERY/RECOVERY_BEGIN` 都遵循同一 Edge admission：证书和 scope 验证 → 有界容量检查 → SQLite 内 request/idempotency durable commit（C4）→ 才允许后续 P2 ring publish。相同 ID/相同 hash 返回已有 acceptance；相同 ID/不同 hash 返回 `IDEMPOTENCY_CONFLICT`；ring 将满时在 C4 前返回 `EDGE_BACKPRESSURE`。Linux ACK 只推进 Edge outbox，永不再次写 native command。

### 5.2 P1 验收

- unit/integration 覆盖 wire codec 兼容/拒绝、双向证书错误、scope/instance 不匹配、证书撤销、断线重连、cursor resume、重复 delivery、ACK 丢失、command backpressure、SQLite writer 崩溃与 outbox replay。
- 所有 command 都必须有 C2/C3/C4；所有 Edge event 都必须有 C11、Linux C12 或明确 delivery failure。跨主机段遵循 `CROSS_HOST_UNPAIRED` 规则。
- 静态敏感扫描、Windows 权限检查与日志检查不得发现 QMT token、账户、证书私钥或命令 payload。
- P1 完成标准是“bridge/Edge 可 fail-close、可重投、可审计”，不是 QMT Native Engine 已可下单。

## 6. P2：SPSC ring、Native Engine 与 Adapter 闭环

### 6.1 设计与实现范围

P2 把已经持久化的 Edge request 以固定二进制帧交给 Big QMT 内置 Python，并将原生 callback 以反向帧返回 Edge。它保留单账户单 native caller，不把 QMT API 引入 Edge Service 或 Linux。

| 文件/单元 | 实现内容 | 必须采集的穿透点 |
| --- | --- | --- |
| `native_engine_contract.py` | 唯一 ABI、slot size/offset、固定 payload、CRC32、commit sequence、role/scope/generation guard | C4/C5/C6/C10。 |
| `deployment/windows-qmt-edge/native_ring.py` | named mapping 优先、P0 允许的 file-backed fallback、SPSC read/write、Interlocked、DACL、预触页、watermark/heartbeat | C5、C10；满/撕裂触发 H0。 |
| `native_engine_client.py` | Edge 唯一 command producer/event consumer；只从 Edge store 读已 durable request，event durable 后才推进 cursor | C4→C5，C10→C11。 |
| `qmt_native_engine.py` | QMT Python 3.6 模型；在 `run_time` drain，固定字段解码，紧邻 native call 的 C7，return/exception C8，callback C9/C10 | C5→C6、C6→C8、C9→C10。 |
| callback/query shim | QMT 原生对象转固定 event payload；订单、成交、撤单、错误、query row、query complete 分开编码 | 每个 native fact 的 C9→C13。 |
| `bridge_normalizer.py` / mapping | Edge durable event → domain fact；route/identity/fact version 去重和冲突拒绝 | C11→C12→C13。 |
| production profile | 使用 P0 manifest + `QmtCapabilityActivation`；fake adapter 不变 | profile activation 不能写进 C0–C13。 |

Native Engine 的热函数只允许：检查已 committed slot → 校验版本/sequence/CRC/scope → 填充固定参数 → `C7` → 原生调用 → `C8` → 写最小结果/故障帧。它禁止 SQL、TLS、JSON、文件枚举、日志格式化、全量查询、策略逻辑、风控计算和任意 Linux 网络访问。callback 也只规范化 fixed event payload 并提交 C10；event ring 无空间、identity 不可关联、异常参数或 native exception 时写最小 fault/gap 并停止接收新单。

### 6.2 P2 验收

1. 模拟 ring fixture 先验证 ABI、commit-last、CRC、sequence wrap、empty/full、wrong role/scope/generation、producer/consumer crash 与重复 event；所有破坏性场景必须生成 H0–H4，不得继续接收新命令。
2. P0 通过后，在单 scope QMT 仿真环境验证 `SUBMIT`、`CANCEL`、`QUERY` 分别产生完整 C0–C8，并将可关联 native facts 走到 C9–C13。
3. 对每个 `C8` 异常、callback 参数异常、event ring full、route conflict、delivery ACK 丢失证明：不重复调用原生函数、不跨 scope 投影、无法证明副作用的 request 为 `UNKNOWN_EFFECT`。
4. 从进程/线程和计数证据证明：每个账户只有一个 `run_time` native caller；没有 Edge/bridge/测试辅助线程直接调用 QMT 原生接口。
5. P2 完成只能表明 Adapter 业务合同在仿真环境打通；仍须 P3 恢复和 P4 性能统一验收，scope 保持 `DISARMED`。

## 7. P3：查询、恢复、故障隔离与受控清理

### 7.1 设计与实现范围

P3 负责在 Linux、Edge 或 QMT Native Engine 任何一个边界故障后，保留未知副作用、阻断新单、重投 durable event，并仅依赖有明确完成语义的 QMT 查询/回显做分类与恢复。

| 实现单元 | 实现内容 | 时间链路 |
| --- | --- | --- |
| `bridge_recovery.py` | attach event stream → resume durable outbox → native query/scan → completion validation → exact route/native classification → evidence | R0→R8；每个 query 同时具备 C0→C13。 |
| Edge recovery service | 根据 request/ring/native result 状态归类 `NOT_PUBLISHED`、`PUBLISHED_UNKNOWN`、`CALL_STARTED_UNKNOWN`、`NATIVE_RESULTED`；禁止新 ID 重发 | C4/C5/C7/C8 与 H0→H2。 |
| identity map / query normalizer | 只保存 callback/query 证明的 native identity；foreign/unattributed/conflict 分离且脱敏 | C9→C13、R5→R6。 |
| health gate | ring、Native Engine、QMT connection、outbox backlog、callback gap、证书和 query completeness 汇总成 stop-new-order reason | H0→H4。 |
| recovery evidence | `BrokerRecoveryEvidence` 记录 query completion、cursor、snapshot/scan digest、分类计数、gap、unknown 与 final gate | R6→R8。 |

恢复固化为：`R0` 阻断新单 → `R1/R2` 保存 cutoff 并挂接 event stream → `R3` 重投 Edge outbox → `R4/R5` 做明确完成的 QMT scan → `R6` 只按 exact route/native identity 分类 → `R7` Journal-first 提交 evidence → `R8` 输出 `RECOVERED_DISARMED` 或 `RECOVERY_BLOCKED`。不自动重报、不自动撤单、不自动解除 DISARMED。人工清理只接受 complete evidence 中唯一、明确属于本 scope 的 native identity。

### 7.2 P3 验收

- 分别在 C4、C5、C7、C8、C10、C11、C12 之后强制退出相应进程；每个边界均保留 request/effect 分类、H0–H4 传播和恢复的 R0–R8。
- 验证 Edge、Native Engine、Linux 各自重启：零重复原生下单、零跨 scope 订单投影、outbox 仅按 delivery 重投、callback gap/unknown 不会被静默清除。
- 验证订单/成交/未完成订单/资产/持仓及适用信用查询的 pagination、末页、空结果、错误、超时和一致性。缺末页、负确认、route 回显、stable identity 或双扫描稳定性任一不足，结论必须为 `RECOVERY_BLOCKED`。
- 恢复成功上限为 `RECOVERED_DISARMED`；需要独立的人工批准、最新风险和发布 gate 才可能尝试新的交易窗口。

## 8. P4：全链路性能、容量与非干扰验收

### 8.1 性能实现范围

P4 在 P0–P3 已通过后才接通 QMT 压测驱动。驱动必须使用仿真账户、事前只读核验、可清理的被动订单、测试专用 profile 和脱敏 archive；不得混入真实生产账户或把历史 Gateway 的 25 ms timer/160 TPS 作为本架构的基线。

| 工具/能力 | 实现要求 | 输出 |
| --- | --- | --- |
| trace collector | 在 C0–C13、R0–R8、H0–H4 写一次性脱敏事实；强制 `null + reason` 表示缺失 | 每笔原始时间对、完整性、sequence 与错误分类。 |
| benchmark driver | 热态单笔、100、300、P0 定义的 max-safe burst；命令与 callback 分别等待、事后限速清理 | 每档逐笔分位数、批次墙钟、计数与清理结果。 |
| clock evidence | 记录 Linux/Windows clock domain、offset/uncertainty 与测试窗口 | 是否允许计算 C3→C4 / C11→C12。 |
| capacity controller | 仅由 P0/P4 的 timer overrun、C5→C6、C6→C8、callback gap、native throttle 与清理结果决定安全 batch/rate | 明确 safe rate 或 `UNDETERMINED`，不从单轮外推。 |
| report generator | 相同 schema 输出 JSON/Markdown，自动标记缺口、unknown、backpressure、throttle 和 abort | 结论与原始 evidence digest 可互相校验。 |

### 8.2 强制报告格式

每个 workload 至少有以下分段。无有效对时跨主机段显示为 `UNPAIRED`，但不得从总报告中删除。

| 层次 | 必须单列的时间 |
| --- | --- |
| Linux 热路径 | `C0→C1`、`C1→C2`、`C0→C2`。 |
| Bridge / Edge | `C2→C3`、`C3→C4`（或 unpaired）、`C4→C5`。 |
| QMT 执行 | `C5→C6`、`C6→C7`、`C7→C8`、`C6→C8`。 |
| callback durable | `C9→C10`、`C10→C11`、`C11→C12`（或 unpaired）、`C12→C13`。 |
| 累计 | `C0→C2`、`C2→C5`、`C5→C8`、`C9→C13`、可关联时的 `C0→C13`。 |
| 批量/系统 | 从共同释放至最后 C8 的 command batch wall-clock；从第一 C9 至最后 C13 的 callback batch wall-clock；`H0→H4`、`R0→R8`。 |

`C0→C13` 只表示某一原生事实回到 OMS 的端到端穿透，不等于成交完成或最终业务状态。首个 order callback、terminal order callback、trade callback、cancel callback、query complete 分别独立统计，严禁相加。

### 8.3 P4 统一验收

1. 每个档位先做 query-only preflight；只有所有必须查询 complete、未完成订单可安全分类、没有待处置 unknown 时才开始 mutation workload。
2. 热态单笔、100、300 与 max-safe burst 每档保留逐笔时间对。max-safe 只能在 P0 给出 timer/native/rate-limit 边界后确定；若尚未确定，报告 `UNDETERMINED`，不以任意大 burst 填充。
3. 每轮必须报告 command `C0→C8`、callback `C9→C13`、可关联累计 `C0→C13`、两类 batch wall-clock、成功/背压/throttle/unknown/gap。有效样本不足、时钟不成立或 callback 缺失均是验收失败/不充分，而非丢弃。
4. 每轮结束等待真实 callback 和查询完成后，以受控限速清理，仅清理本 scope 且可精确关联的订单；最终 query-only 核验、callback gap、outbox backlog 和 unknown-effect 都必须归零或明确阻断。
5. 固定环境至少执行三轮；变化 CPU affinity、timer period、batch 或 QMT build 时重新建基线。结论只适用于记录的 scope、QMT build、manifest、Edge build、设备和网络窗口。

## 9. 实施顺序、代码提交与统一验收门禁

每个功能包内部允许先完成设计与代码，再在该功能包结束时进行一次集中验收；不得把中途单测、单笔 native return 或普通网络连通性标为完成。每次代码修改独立提交并推送当前 `dev` 分支，提交信息需指明 P 包和功能边界；真实 QMT evidence 只归档脱敏摘要及 digest，不提交账户或原始敏感载荷。

```text
P0 capability / manifest / ABI probe
  → P0 集中真实仿真验收，manifest 或 fail evidence 冻结
  → P1 bridge / Edge durable service
  → P1 集中 protocol、mTLS、ACL、outbox、backpressure 验收
  → P2 ring / Native Engine / adapter 业务闭环
  → P2 集中 submit、cancel、query、callback 与 ABI 验收
  → P3 fault / recovery / controlled cleanup
  → P3 集中 crash matrix 与 recovery evidence 验收
  → P4 trace / benchmark / capacity
  → P4 集中性能、清理、非干扰与发布评审
```

| Gate | 必须同时通过 | 失败时动作 |
| --- | --- | --- |
| G0（P0） | manifest 签名、route 字段、native context、callback/query completion、ring ABI/ACL/Interlocked、原始时间对 | `CAPABILITY_UNAVAILABLE`；不启动 Native Engine。 |
| G1（P1） | mTLS mutual auth、scope fence、certificate/revocation、bounded bridge、SQLite/outbox crash/replay、H0→H4 | Edge not-ready；不向 ring 发布。 |
| G2（P2） | fixed ABI、single caller、C0–C13 事实、callback normalization、ring crash/full/gap、unknown-effect | `DISARMED`；不以 C8 表示成功。 |
| G3（P3） | R0–R8、query completion、exact identity、zero duplicate/foreign projection、controlled cleanup | `RECOVERY_BLOCKED` 或 `RECOVERED_DISARMED`；绝不 READY。 |
| G4（P4） | 三轮稳定 workload、完整原始时间对、clock evidence、gap/unknown/cleanup 门禁、容量证据 | 不发布性能/SLA，保留诊断 evidence。 |

## 10. 最终交付物与验收归档

每包结束时必须归档下列脱敏项目，并由结果生成器交叉校验 hash：

1. 精确 Git revision、QMT/Edge/Native Engine build digest、capability/route/ABI manifest digest、scope/adapter instance digest；
2. P0 capability 结论、P1 bridge/ACL 结论、P2 C0–C13 原始 trace、P3 R0–R8/H0–H4 fault trace、P4 聚合性能结果；
3. 每种完成/失败/不充分状态的计数、错误类别、callback gap、outbox backlog、unknown-effect 和 cleanup 摘要；
4. 时钟域与跨主机 offset/uncertainty evidence；
5. 仅可复现所需的公开配置摘要。不得归档账户、标的、价格、订单号、证书私钥、token、IP、密码或完整命令 payload。

满足 G4 后也只形成“该版本、该仿真环境的 QMT Adapter 验收与性能结论”。是否解除 `DISARMED` 必须另行在真实启用窗口评审当前 manifest、人工批准、风险输入、账户状态和所有 runtime health，且不由本文件自动授权。
