# Mermaid Live 图代码

> 当前状态依据：[当前未实施功能与优化收口路线](../01-架构与定档/10-当前未实施功能与优化收口路线.md)及[BenchmarkStandardV1 复验报告](../04-测试验收与性能/04-OMS性能与容量/05-BenchmarkStandardV1复验报告-2026-09-11.md)。
> 使用方式：将任一 `mermaid` 代码块完整复制到 [Mermaid Live Editor](https://mermaid.live)，即可编辑并导出 SVG/PNG。
> 图示口径（2026-09-11）：EMT-only OMS/EMS、mmap Journal、ConsumerGateway、EMT profile 与 OMS W7 已完成验收；图内 PaperBroker/QMT、跨主机 HA 与 `READY` 是未完成扩展。图 5 是历史阶段路线，不能被误读为当前仍处于 P0。
> 风格：统一使用黑白 `base`、ELK 与 classic。PNG 文件由本文件对应图源导出。

## 图 1：目标产品总体架构

适用于开场说明模块边界、三层持久化和 EMT/QMT/PaperBroker 的可插拔关系。对应导出文件：`emt_gateway总体架构.png`。

```mermaid
---
config:
  theme: base
  themeVariables:
    background: '#ffffff'
    primaryColor: '#ffffff'
    primaryTextColor: '#000000'
    primaryBorderColor: '#000000'
    lineColor: '#000000'
    secondaryColor: '#ffffff'
    tertiaryColor: '#ffffff'
  layout: elk
  look: classic
---
flowchart LR
    subgraph CONSUMER[内部消费者侧]
        direction TB
        USER[消费者登录<br/>授权账户名称]
        STRATEGY[策略 / 受控人工操作]
    end

    subgraph PRODUCT[emt_gateway 目标产品]
        direction TB
        CG[ConsumerGateway<br/>binding、回执、cursor、审计]
        OMS[OMS 订单簿<br/>幂等、归属、冻结、状态、对账]
        EMS[EMS 执行层<br/>路由、保护、lane、会话、Kill Switch]
        REG[VenueAdapterRegistry<br/>profile 与 capability/mapping manifest]
        ADAPTER[BrokerAdapter<br/>submit、cancel、query、recovery evidence]
    end

    subgraph FACTS[每账户事实与异步副本]
        direction TB
        HOT[(PersistentHotJournal<br/>同步可恢复事实)]
        SSD[(SSD sealed Journal<br/>异步耐久副本)]
        PG[(PostgreSQL<br/>异步可重建 read model)]
    end

    subgraph VENUE[独立柜台 profile]
        direction TB
        EMT[EmtBrokerAdapter<br/>EMT SDK / 柜台]
        QMT[QmtBrokerAdapter<br/>QMT SDK / 柜台]
        PAPER[PaperBrokerAdapter<br/>确定性撮合 / 回放]
    end

    USER --> STRATEGY --> CG --> OMS --> EMS --> REG --> ADAPTER
    ADAPTER --> EMT
    ADAPTER --> QMT
    ADAPTER -. 回归 / 压测 .-> PAPER
    OMS -->|归属、冻结、CALL_STARTED<br/>Event batch 先 commit| HOT
    HOT -. 异步复制 .-> SSD
    HOT -. 异步投影 .-> PG
    EMT -->|规范化 BrokerEvent| OMS
    QMT -->|规范化 BrokerEvent| OMS
    PAPER -->|规范化 BrokerEvent| OMS

    style USER fill:#ffffff,stroke:#000000,color:#000000
    style STRATEGY fill:#ffffff,stroke:#000000,color:#000000
    style CG fill:#ffffff,stroke:#000000,color:#000000
    style OMS fill:#ffffff,stroke:#000000,color:#000000
    style EMS fill:#ffffff,stroke:#000000,color:#000000
    style REG fill:#ffffff,stroke:#000000,color:#000000
    style ADAPTER fill:#ffffff,stroke:#000000,color:#000000
    style HOT fill:#ffffff,stroke:#000000,color:#000000
    style SSD fill:#ffffff,stroke:#000000,color:#000000
    style PG fill:#ffffff,stroke:#000000,color:#000000
    style EMT fill:#ffffff,stroke:#000000,color:#000000
    style QMT fill:#ffffff,stroke:#000000,color:#000000
    style PAPER fill:#ffffff,stroke:#000000,color:#000000
    style CONSUMER fill:#ffffff,stroke:#000000,color:#000000
    style PRODUCT fill:#ffffff,stroke:#000000,color:#000000
    style FACTS fill:#ffffff,stroke:#000000,color:#000000
    style VENUE fill:#ffffff,stroke:#000000,color:#000000
```

## 图 2：消费者信号到柜台回调的 C0–C7 事实链

适用于说明 receipt、柜台受理和订单终态是不同阶段，以及为何副作用前/回调后都必须先 Journal。原生 callback 允许先于同步返回到达，必须在线性化收敛后再推进状态；满环不允许静默丢 callback：它形成可审计缺口和 `CALLBACK_GAP` gate，必须由恢复证据覆盖。EMT 是首个 profile，QMT 遵循同一契约。对应导出文件：`订单到EMT回调的端到端链路.png`。

```mermaid
---
config:
  theme: base
  themeVariables:
    background: '#ffffff'
    primaryColor: '#ffffff'
    primaryTextColor: '#000000'
    primaryBorderColor: '#000000'
    lineColor: '#000000'
    secondaryColor: '#ffffff'
    tertiaryColor: '#ffffff'
  layout: elk
  look: classic
---
flowchart TD
    S[C0 策略信号<br/>binding 已验证]
    O[C1 OMS 准入<br/>幂等、风险、归属、Reservation]
    J1[(C2 PersistentHotJournal<br/>ORDER_ATTRIBUTED + RESERVATION_CREATED + CALL_STARTED)]
    E[C3 EMS execution lane<br/>价格/频率保护、分配 client_order_ref]
    N[跨越 SDK 调用边界<br/>EMT InsertOrder / QMT 等价调用]
    A[C4 原生返回观察<br/>可晚于 C5]
    V[柜台 / 交易所]
    SPI[C5 callback ingress<br/>EMT fastspi v2 / 其他 profile 等价 ingress]
    RACE[NativeOutcomeRace<br/>合并 C4/C5；callback 可先到]
    W[C6 callback worker<br/>source_key 去重、状态/冻结调整]
    J2[(Event Journal BatchCommit<br/>BROKER_EVENT + Reservation 调整)]
    CE[C7 ConsumerEvent<br/>cursor 可重放的本地确认]
    GAP[ring 满 / 入队失败<br/>CallbackGapLatch：CALLBACK_GAP + RECONCILIATION gate]
    GJ[(CALLBACK_GAP_DETECTED<br/>Journal 事实)]
    R[IntentReceipt<br/>accepted_event_id]
    U[SUBMIT_UNKNOWN / RECONCILE_REQUIRED<br/>禁止自动副作用，进入对账]

    S --> O --> J1 --> E --> N --> V
    N -->|C4 返回观察| A --> RACE
    V -->|订单 / 成交 / 撤单错误回调| SPI --> RACE --> W --> J2 --> CE
    J1 -. CALL_STARTED 已 durable .-> R
    A -. 无 callback 可证明时返回不明/调用边界崩溃 .-> U
    W -. 重复、乱序、未知或冲突 .-> U
    SPI -. 满环：不计 C5 .-> GAP
    GAP -->|容量恢复后先提交| GJ
    GJ -. 完整回放/查询覆盖前 .-> U

    style S fill:#ffffff,stroke:#000000,color:#000000
    style O fill:#ffffff,stroke:#000000,color:#000000
    style J1 fill:#ffffff,stroke:#000000,color:#000000
    style E fill:#ffffff,stroke:#000000,color:#000000
    style N fill:#ffffff,stroke:#000000,color:#000000
    style A fill:#ffffff,stroke:#000000,color:#000000
    style V fill:#ffffff,stroke:#000000,color:#000000
    style SPI fill:#ffffff,stroke:#000000,color:#000000
    style RACE fill:#ffffff,stroke:#000000,color:#000000
    style W fill:#ffffff,stroke:#000000,color:#000000
    style J2 fill:#ffffff,stroke:#000000,color:#000000
    style CE fill:#ffffff,stroke:#000000,color:#000000
    style GAP fill:#ffffff,stroke:#000000,color:#000000
    style GJ fill:#ffffff,stroke:#000000,color:#000000
    style R fill:#ffffff,stroke:#000000,color:#000000
    style U fill:#ffffff,stroke:#000000,color:#000000
```

## 图 3：OMS / EMS 订单与撤单状态机

适用于回答同步返回为什么不是最终成交，以及崩溃/结果不明时为什么不能重报。对应导出文件：`API订单命令状态机.png`。

```mermaid
---
config:
  theme: base
  themeVariables:
    background: '#ffffff'
    primaryColor: '#ffffff'
    primaryTextColor: '#000000'
    primaryBorderColor: '#000000'
    lineColor: '#000000'
    secondaryColor: '#ffffff'
    tertiaryColor: '#ffffff'
  layout: elk
  look: classic
---
flowchart TD
    RECEIVED[SubmitSignal]
    REJECTED[REJECTED]
    COMMAND[COMMAND_DURABLE<br/>ORDER_ATTRIBUTED + RESERVATION_CREATED + CALL_STARTED]
    UNKNOWN[SUBMIT_UNKNOWN]
    REC[RECONCILE_REQUIRED<br/>禁止自动重报/撤单/释放冻结]
    OBSERVED[柜台事实已耐久<br/>WORKING / PARTIALLY_FILLED / FILLED / CANCELED / REJECTED]
    RECOVERED[仅完整回放 + 查询 + 对账证据<br/>收敛至柜台可证明状态]

    subgraph OUTCOME[原生返回与 callback 收敛]
        direction LR
        RETURN[NATIVE_ACCEPTED<br/>C4 返回先到且 durable]
        CALLBACK[NATIVE_OBSERVED_FROM_CALLBACK<br/>C5 callback 先到且 durable]
        LATE[NATIVE_RETURN_OBSERVED<br/>迟到 C4 仅补审计]
        CALLBACK --> LATE
        RETURN --> OBSERVED
        CALLBACK --> OBSERVED
    end

    subgraph CANCEL[撤单事实链]
        direction LR
        CD[CANCEL_COMMAND_DURABLE<br/>CANCEL_REQUESTED + CANCEL_STARTED]
        CP[CANCEL_PENDING<br/>venue_cancel_id durable]
        CU[CANCEL_SUBMIT_UNKNOWN]
        CR[CANCEL_REJECTED]
        CANCELED[柜台确认 CANCELED]
        CD --> CP --> CANCELED
        CD --> CU
        CP --> CR
    end

    RECEIVED -->|binding / 风控 / 幂等冲突| REJECTED
    RECEIVED --> COMMAND
    COMMAND -->|C4 返回先到| RETURN
    COMMAND -->|C5 callback 先到| CALLBACK
    COMMAND -->|返回不明/崩溃/commit 不明| UNKNOWN --> REC
    COMMAND -->|证明未跨越原生调用| REJECTED

    OBSERVED -->|仅 WORKING / PARTIALLY_FILLED 可发撤单| CD
    CU --> REC
    CR -->|未证明仍可工作| REC
    REC --> RECOVERED

    style RECEIVED fill:#ffffff,stroke:#000000,color:#000000
    style REJECTED fill:#ffffff,stroke:#000000,color:#000000
    style COMMAND fill:#ffffff,stroke:#000000,color:#000000
    style UNKNOWN fill:#ffffff,stroke:#000000,color:#000000
    style REC fill:#ffffff,stroke:#000000,color:#000000
    style OBSERVED fill:#ffffff,stroke:#000000,color:#000000
    style RECOVERED fill:#ffffff,stroke:#000000,color:#000000
    style RETURN fill:#ffffff,stroke:#000000,color:#000000
    style CALLBACK fill:#ffffff,stroke:#000000,color:#000000
    style LATE fill:#ffffff,stroke:#000000,color:#000000
    style CD fill:#ffffff,stroke:#000000,color:#000000
    style CP fill:#ffffff,stroke:#000000,color:#000000
    style CU fill:#ffffff,stroke:#000000,color:#000000
    style CR fill:#ffffff,stroke:#000000,color:#000000
    style CANCELED fill:#ffffff,stroke:#000000,color:#000000
    style OUTCOME fill:#ffffff,stroke:#000000,color:#000000
    style CANCEL fill:#ffffff,stroke:#000000,color:#000000
```

## 图 4：崩溃恢复、查询与对账闸门

适用于说明“以 EMT 回调为准”的精确定义：是柜台 Adapter 的完整恢复证据，不是单条回调；callback gap 也必须得到回放/查询覆盖。持久内存物理损坏且 SSD 后缀不完整时，必须断开旧事件流并经人工建立新 generation；旧 cursor 还必须通过稳定控制根中的保留 key/registry 验证，控制根不可证明则 fail-closed。完全一致也只能到 `DISARMED`。对应导出文件：`查询接口的调用与完成语义.png`。

```mermaid
---
config:
  theme: base
  themeVariables:
    background: '#ffffff'
    primaryColor: '#ffffff'
    primaryTextColor: '#000000'
    primaryBorderColor: '#000000'
    lineColor: '#000000'
    secondaryColor: '#ffffff'
    tertiaryColor: '#ffffff'
  layout: elk
  look: classic
---
flowchart TD
    A[进程启动 / 获得 AccountOwnershipFence] --> B[RECONCILIATION gate + BOOTSTRAP]
    B --> C[校验 V1 anchor<br/>SSD sealed 前缀 + committed hot 后缀]
    C -->|连续且可验证| D[仅从 Journal 重建<br/>易失 OMS、冻结、归属、待决指令]
    C -->|gap / 半批 / 密钥或持久化域错误| X[独立运维诊断<br/>RECONCILE_REQUIRED]
    D --> E[Adapter: attach durable ingress<br/>begin_recovery、回放、只读分页快照]
    E --> F[BrokerRecoveryEvidence<br/>覆盖、终态、as-of/窗口、能力版本]
    Q[CallbackGapEvidence<br/>含崩溃前未落 Journal 的保守缺口] --> F
    F --> G{本地事实、柜台证据与全部 gap<br/>均一致、完整且已覆盖关闭?}
    G -->|是| H[RECOVERY_EVIDENCE_CAPTURED<br/>RECOVERY_VALIDATED → DISARMED]
    G -->|否| X
    X -->|PMem 丢失且 SSD 后缀不完整| Y[旧 Journal 只读<br/>保留 key + registry 无环解析后 JOURNAL_GAP + reset context<br/>当前尾流 snapshot → 新 cursor；人工重新 seed client ref]
    H --> I[受控人工审批 ARM]
    I --> J[READY]

    style A fill:#ffffff,stroke:#000000,color:#000000
    style B fill:#ffffff,stroke:#000000,color:#000000
    style C fill:#ffffff,stroke:#000000,color:#000000
    style D fill:#ffffff,stroke:#000000,color:#000000
    style E fill:#ffffff,stroke:#000000,color:#000000
    style F fill:#ffffff,stroke:#000000,color:#000000
    style Q fill:#ffffff,stroke:#000000,color:#000000
    style G fill:#ffffff,stroke:#000000,color:#000000
    style H fill:#ffffff,stroke:#000000,color:#000000
    style I fill:#ffffff,stroke:#000000,color:#000000
    style J fill:#ffffff,stroke:#000000,color:#000000
    style X fill:#ffffff,stroke:#000000,color:#000000
    style Y fill:#ffffff,stroke:#000000,color:#000000
```

## 图 5：历史 P0→M5 产品路线（非当前状态图）

适用于项目计划和资源申请。对应导出文件：`SDK能力门禁与上线判定.png`；文件名因兼容既有引用保留，但图示内容是完整产品门禁。

```mermaid
---
config:
  theme: base
  themeVariables:
    background: '#ffffff'
    primaryColor: '#ffffff'
    primaryTextColor: '#000000'
    primaryBorderColor: '#000000'
    lineColor: '#000000'
    secondaryColor: '#ffffff'
    tertiaryColor: '#ffffff'
  layout: elk
  look: classic
---
flowchart TD
    P0[P0 当前基础设施<br/>Gateway、V1 Journal、callback ingress、异步 PG、Linux CI]
    M0[M0 稳定边界<br/>domain、Consumer/Adapter 契约、mapping、兼容层]
    H0[H0 持久化 Go/No-Go<br/>目标 Linux、PMem DAX、SSD、真实恢复]
    M1[M1 OMS 内核<br/>订单事实、冻结、V2 Hot Journal、异步 SSD/PG]
    M2[M2 单账户 EMS + EMT<br/>execution lane、归属、fastspi v2、保护、可靠事件]
    M3[M3 恢复与对账<br/>回放、分页快照、外部单、受控控制面]
    M4[M4 PaperBroker<br/>确定性回放、故障注入、300/s 正确性压测]
    M5[M5 多消费者 / 多账户 / 多 profile<br/>QMT、算法、归档、迁移演练]
    R0[R0 发布候选与受控上线<br/>目标主机、恢复演练、每 profile 仿真验证、按账户启用]
    DONE[产品完整落地<br/>M0–M5 与 R0 全部通过]
    STOP[DISARMED / RECONCILE_REQUIRED<br/>禁止自动重报与自动放行]

    P0 --> M0 --> H0 --> M1 --> M2 --> M3 --> M4 --> M5 --> R0 --> DONE
    H0 -. 持久化域不通过 .-> STOP
    M1 -. 事实或恢复失败 .-> STOP
    M2 -. Adapter / 保护失败 .-> STOP
    M3 -. 对账差异 .-> STOP
    M5 -. profile 迁移不满足条件 .-> STOP
    R0 -. 任一受控验证失败 .-> STOP

    style P0 fill:#ffffff,stroke:#000000,color:#000000
    style M0 fill:#ffffff,stroke:#000000,color:#000000
    style H0 fill:#ffffff,stroke:#000000,color:#000000
    style M1 fill:#ffffff,stroke:#000000,color:#000000
    style M2 fill:#ffffff,stroke:#000000,color:#000000
    style M3 fill:#ffffff,stroke:#000000,color:#000000
    style M4 fill:#ffffff,stroke:#000000,color:#000000
    style M5 fill:#ffffff,stroke:#000000,color:#000000
    style R0 fill:#ffffff,stroke:#000000,color:#000000
    style DONE fill:#ffffff,stroke:#000000,color:#000000
    style STOP fill:#ffffff,stroke:#000000,color:#000000
```

## 使用与导出规则

- 汇报主线建议：图 1 说明模块边界，图 2 说明热路径事实链，图 3 回答订单状态与重报限制，图 4 说明崩溃恢复，图 5 确认阶段门禁。
- PPT 优先导出 SVG；如使用 PNG，必须由本文件图源重新导出，保留图源 Git SHA。
- 图中的 PaperBroker、QMT、跨主机 HA 与 `READY` 均为未完成扩展；EMT-only OMS/EMS、ConsumerGateway、mmap Journal 与 OMS sidecar 已有对应验收。图 5 仅用于说明早期路线，当前范围以收口路线为准。
- 图 4 的 `RECOVERY_VALIDATED → DISARMED` 不等同账户可交易；只有经控制面审批 `ARM` 后才可 `READY`。
