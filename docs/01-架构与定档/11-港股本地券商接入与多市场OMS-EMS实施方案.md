# 港股本地券商接入与多市场 OMS-EMS 实施方案

> 状态：**方案定档候选，尚未实施。** 本文件用于评估港股本地券商接入对 TradeCore OMS、EMS 与 PostgreSQL read model 的影响，并给出推荐的实施与验收顺序。它不证明 EMT 已支持港股本地交易，不授权连接任意券商，也不改变任何账户的 `DISARMED` 状态。
>
> 背景来源：2026-09 的《关于东财 EMT、港股支持接入计划》提出“东财个人量化 API”与“香港本地券商账户/量化权限”两条独立准备线，且尚未确定港股券商和 SDK。本文中的港股适配器是占位名称，必须以最终获得的官方 SDK、账户权限和 capability probe 为准。

## 1. 结论

### 1.1 推荐架构

**不拆分 `oms_ashare` 与 `oms_hk` 两套代码。** 推荐“一个通用 OMS 代码和统一 read model，多个隔离的 execution scope/OMS actor，初期多个独立部署实例”的混合模式：

```text
策略信号
  │
  ▼
签名 MarketRoutePlan
  │  精确选择；禁止按 market 猜券商或自动 failover
  ├── A 股 route  → ExecutionScope(EMT, account, client scope) → EMT EMS Adapter
  └── 港股 route  → ExecutionScope(HK broker, account, client scope) → HK EMS Adapter
                       │
                       ├─ 独立 Journal / callback ingress / native session
                       ├─ 独立风险与资金冻结账本
                       └─ 独立进程、CPU/cgroup 与故障域（首发）

统一 OMS Core / 统一 PostgreSQL read model / 统一 Consumer API
```

“一个 OMS”指的是一套领域代码、状态机、Journal 事实模型、审计和查询模型，**不**意味着所有市场在同一个共享 `OrderBook`、共享现金桶或同一个 SDK session 中运行。首发应保持 A 股与港股独立运行实例；两者只共享版本化 contracts、read model schema 和受控控制面。

### 1.2 能否只适配 EMS

可以先做 EMS-only 的连通性/模拟盘 PoC，但它不足以安全承载真实港股交易。当前 OMS 对港股存在中等且必要的通用化改造：多币种资金、市场化持仓键、账户执行 scope 和市场规则必须进入 OMS 风险与事实边界。

因此，推荐结论是：

> 港股接入不能只增加券商 connector 后直接上线。TradeCore 的 Adapter/EMS 边界已具备多券商基础，新增港股券商的主要工作在 EMS；但 OMS 当前仍有 CNY 单币种、按 symbol 聚合冻结、按 account ID 路由的 A 股假设。应先完成一次中等规模的通用 scope 与多币种 OMS 改造，再以独立 EMS adapter 接入港股券商。无需维护 `oms_ashare`、`oms_hk` 两套代码；首发采用同一 OMS 核心的 A/HK 独立实例，风险更低。

### 1.3 EMT 与港股的边界

当前仓库中已证实的 EMT mapping manifest 只包含 `SZ`、`SH` 两个 market 映射。EMT 文档矩阵中的港股通参考查询不是港股本地证券账户的下单、撤单、回调和恢复能力证明。除非未来取得 EMT 官方港股交易 SDK、字段映射、账户权限与完整 capability evidence，否则港股必须实现为独立的 `HK_<BROKER>` Adapter，而不是在 EMT Adapter 中虚构 `HK` market code。

## 2. 三种选择的比较

| 选择 | 短期开发量 | 正确性/运维风险 | 长期成本 | 结论 |
| --- | --- | --- | --- | --- |
| 只增加 EMS 港股 connector，不调整 OMS | 低 | 高：CNY 冻结、持仓与 route 会产生跨市场歧义 | 后续必然返工 | 仅限 PoC，不可作为真实交易方案。 |
| `oms_ashare`、`oms_hk` 两套独立代码/表/状态机 | 初期中等 | 中等：隔离明确，但修复、恢复、审计和策略 API 容易分叉 | 高 | 不推荐。 |
| 通用 OMS + 独立 scope + 独立港股 EMS Adapter | 中等 | 低：准确隔离资金、持仓、session 与恢复 | 最低 | **推荐。** |

## 3. 当前代码基线与改造影响

### 3.1 已有的可复用基础

| 层次 | 当前能力 | 港股可复用性 |
| --- | --- | --- |
| 领域/订单 | `OmsOrderIntent`、`ExecutionInstruction` 已有 `market`、symbol、价格 ticks、订单状态与不可变 trace。 | 可复用，但 `market` 目前只是字符串，不能单独决定券商、币种或规则。 |
| Adapter/EMS | `BrokerAdapter` 已抽象 submit/cancel/query/recovery，mapping/capability manifest 与 callback observer 均是 venue-neutral。 | 高；新增港股 broker adapter 应使用该合同。 |
| 执行隔离 | execution runtime、session supervisor、scheduler、Journal 和 callback gap 具有账户/adapter 隔离意识。 | 可复用，但路由当前主要按 `account_id`，需改为 scope key。 |
| OMS read model | PostgreSQL 已存储 `venue_id`、`adapter_instance_id`、`market`、`currency`。 | 高；可演进为多市场 schema，不需拆库。 |
| 发布/恢复 | 签名 manifest、`DISARMED`、host-health、Journal-first、manual-only recovery 已建立。 | 高；港股必须重新证明，不得复用 EMT evidence。 |

### 3.2 必须改造的 OMS 假设

| 当前假设 | 具体影响 | 港股正确处理 |
| --- | --- | --- |
| `ReservationRequirement.currency` 仅允许 `CNY` | HKD 资金冻结会在 OMS 入场即失败，或被错误换算为 CNY。 | 资金冻结改为按 ISO currency 的 fixed-point 金额；禁止隐式 FX。 |
| 可卖/可借数量只按 `symbol` 查找和聚合 | 相同代码文本、不同市场或不同币种/账户可产生错误冻结。 | 使用 `(market_id, instrument_id, position_direction)` 作为库存键。 |
| `OrderBook`、EMS router、scheduler 主要按 `account_id` 分区 | 同逻辑账户下的不同 venue/client scope 无法安全共存；回调可能误路由。 | 使用不可变 `ExecutionScopeKey` 分区。 |
| 订单仅以字符串 `market` 表达市场 | 无法校验交易时段、板块、lot、tick、订单类型、货币和结算规则。 | 用签名 `MarketRoute` 与 `MarketRuleBundle` 约束订单。 |
| 风险快照只有单一 cash/margin 与按 symbol 的库存 map | 无法表达 HKD/CNY 资金、市场化持仓、费用和结算可卖性。 | 采用按 currency、instrument key、风险版本的不可变快照。 |
| A 股与 EMT 的静态 mapping 已验收 | 不代表任何港股券商的字段、回调、限流、撤单或恢复语义。 | 新港股 adapter 重新编制 mapping/capability/recovery manifest。 |

上述改造属于**中等 OMS 工作量**：要修改领域 key、风险冻结和 execution routing，但不需要复制订单状态机、Journal、consumer event、projector 或审计系统。

## 4. 目标领域模型

### 4.1 `ExecutionScopeKeyV1`

新增内部不可变 scope，作为 native session、Journal、恢复、风险和 callback 的共同隔离键：

```text
ExecutionScopeKeyV1
  account_id
  venue_id                    # EMT / FUTU_HK / TIGER_HK / IBKR 等
  adapter_instance_id
  adapter_client_scope_digest # 不在 read model 明文保存原生 session/client 值
  scope_hash                  # 上述 canonical encoding 的 SHA-256
```

一个 scope 可以支持多个 market，例如同一券商账户的 HKEX 和其他已证明市场；一个 market 也可以由多个 scope 承载。它不等于“市场”，更不等于“策略”。每个 scope 必须具有唯一 Journal generation、callback ingress、fence、session owner 和 recovery evidence。

### 4.2 `MarketRouteV1`

市场选择是控制面/启动期加载的签名配置，而不是策略可自由填写的字符串：

```text
MarketRouteV1
  route_id / version / signed_release_hash
  market_id                   # SSE / SZSE / HKEX
  asset_class                 # EQUITY / ETF / OPTION ...
  execution_scope_hash
  settlement_currency         # CNY / HKD
  trading_calendar_id
  market_rule_bundle_hash
  adapter_capability_hash
  valid_from / valid_until / revocation reference
```

策略信号只表达被允许的 instrument/side/quantity/limit；OMS 在 Journal 前把其绑定为一个不可变 `MarketRouteV1`。路由不存在、市场规则过期、券商 capability 不匹配或 scope 不可用时必须在 C0/C1 前拒绝，绝不改投其他券商。

### 4.3 `InstrumentKeyV1`、资金与持仓

```text
InstrumentKeyV1 = market_id + asset_class + canonical_instrument_id
PositionKeyV1   = ExecutionScopeKey + InstrumentKeyV1 + direction
CashKeyV1       = ExecutionScopeKey + currency
```

- 所有金额继续使用 fixed-point 整数；currency exponent、价格 scale、tick 表和费用规则由 `MarketRuleBundle` 固定，禁止在 C0--C7 查询外部行情或规则服务。
- 不做自动 CNY/HKD 换汇。跨币种余额、保证金、FX 折算和汇率有效期必须由单独、版本化的风险快照提供；缺失则拒绝新单。
- 港股 lot、odd-lot、tick table、订单类型、交易时段、半日市/恶劣天气、结算与可卖数量均为 market-rule 数据，不能沿用 A 股固定手数、价格或 T+1 假设。
- OMS 统一状态机仍可使用 `ACCEPTED → WORKING → PARTIALLY_FILLED → terminal`；券商私有状态保留在 Adapter 的受限 normalized summary，不得污染跨市场领域状态机。

## 5. EMS 与 Adapter 实施方案

### 5.1 港股 Adapter 的包边界

在最终券商确定前，仅预留如下边界：

```text
xuanling_tradecore/adapters/
  emt/                 # 继续只承载 EMT 已证明市场
  hk_<broker>/          # 新建：SDK transport、mapping、normalizer、profile
    capabilities.py
    mapping.py
    session_adapter.py
    event_ingress.py
    read_only_ingress.py
    route_token.py
    profile.py
```

港股 Adapter 必须独立提供：

1. 订单、撤单、订单/成交/撤单错误回调的 native mapping；
2. 稳定 client reference、venue order ID、trade ID 与本系统归属字段的实测回显；
3. 资产、持仓、未完成订单、订单、成交、分页与回放的完成语义；
4. session/thread model、rate limit、登录/断线/重连和 callback ownership；
5. 港股账户的币种、可卖量、费用、融资/卖空及权限 capability；
6. 只读 recovery evidence；未证明完整性前一律 manual-only、`DISARMED`。

### 5.2 EMS 路由改造

`ExecutionInstruction` 需要持久绑定 `execution_scope_hash`、`market_route_hash` 和 market-rule bundle hash。`AccountExecutionRouter`、scheduler、session supervisor、kill switch、inflight credit 和 callback route 的 key 从裸 `account_id` 升级为 `ExecutionScopeKeyV1`。

这不会把多 market 的 SDK 调用塞进同一个 session：每个 Adapter scope 仍坚持一个 native caller；市场或券商切换只能发生在新订单的受控路由阶段，既有订单/撤单永远沿已持久化的 native identity 返回原 scope。

## 6. PostgreSQL read model 修改方案

### 6.1 原则

现有九表 read model 已有 `venue_id`、`adapter_instance_id`、`market`、`currency` 字段，适合演进；但其账户/资产/持仓/成交主键没有完整 adapter client scope，不能安全表示同一账户在多个 execution scope 下的并行事实。

PostgreSQL 永远是异步 read model：迁移、backfill、dual projection、索引建立和报表查询均不得进入 C0--C7，也不得成为下单/撤单/回调确认前置条件。

### 6.2 新增表

```sql
CREATE TABLE xuanling_tradecore_rm.execution_scopes (
    execution_scope_id uuid PRIMARY KEY,
    scope_hash xuanling_tradecore_rm.sha256_hex UNIQUE NOT NULL,
    account_id text NOT NULL,
    venue_id varchar(32) NOT NULL,
    adapter_instance_id text NOT NULL,
    adapter_client_scope_digest xuanling_tradecore_rm.sha256_hex NOT NULL,
    release_manifest_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    active boolean NOT NULL,
    revision bigint NOT NULL,
    source_fact_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE xuanling_tradecore_rm.market_routes (
    market_route_id uuid PRIMARY KEY,
    execution_scope_id uuid NOT NULL
        REFERENCES xuanling_tradecore_rm.execution_scopes(execution_scope_id),
    market_id varchar(32) NOT NULL,
    asset_class varchar(32) NOT NULL,
    settlement_currency char(3) NOT NULL,
    trading_calendar_id varchar(64) NOT NULL,
    market_rule_bundle_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    capability_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    valid_from timestamptz NOT NULL,
    valid_until timestamptz,
    revoked_at timestamptz,
    revision bigint NOT NULL,
    source_fact_hash xuanling_tradecore_rm.sha256_hex NOT NULL
);
```

`execution_scopes` 与 `market_routes` 是 Journal/release manifest 的异步投影，不允许由 SQL 管理员直接作为交易路由来源修改。

### 6.3 九表的 V2 调整

| 表 | 必须增加/调整 | 原因 |
| --- | --- | --- |
| `accounts` | `execution_scope_id`；账户唯一性从 `(account_id, venue_id, adapter_instance_id)` 演进为 scope identity。 | 同账户/venue/adapter 的不同 client scope 不得冲突。 |
| `orders` | `execution_scope_id`、`market_route_id`、`market_rule_bundle_hash`。保留策略订单号的全局幂等唯一性。 | 同一个 strategy order ID 若被重新路由到另一市场，必须成为 conflict，不能双重下单。 |
| `order_executions` | `execution_scope_id`、`market_route_id`；所有 native ID 唯一索引包含 scope。 | 确保回调和撤单回到原始 Adapter session。 |
| `order_actions` | 通过 execution/order 外键获得 scope；为高频审计查询可冗余 `execution_scope_id` 并以约束校验。 | 防止跨 scope 撤单或修改。 |
| `trades` | `execution_scope_id`、`market_route_id`；trade-key 唯一索引包含 scope。 | 不同券商的 trade ID 不能冲突。 |
| `assets` | 主键/唯一索引包含 `execution_scope_id + currency`。 | 多币种资产不能互相覆盖。 |
| `positions` | 主键包含 `execution_scope_id + market + instrument + direction`；新增 `settled_qty`、`unsettled_qty`、`sellable_qty`。 | 不把 A 股 `today/yesterday` 语义强加给港股。 |
| `projection_checkpoints` | `account_scope` 替换为稳定 `execution_scope_id/scope_hash`。 | projector watermark 与 Journal generation 必须 scope-local。 |
| `strategies` | 不按市场分表；可新增只读授权关联表 `strategy_market_routes`。 | 策略身份保持跨市场一致，交易权限由 route plan 限定。 |

现有 `market`、`instrument_id`、`currency`、`venue_id` 字段继续保留，不能改名伪造语义一致；新字段只增加可验证的 scope/route/rule provenance。

### 6.4 迁移步骤

1. **离线设计与迁移 002**：新增 `execution_scopes`、`market_routes` 和所有 V2 nullable scope/route 列及索引；不修改现有历史行，不自动 DDL。
2. **受控 backfill**：在所有相关账户 `DISARMED` 时，用已验证 Journal/release manifest 为历史 EMT 行生成唯一默认 execution scope；输出 count/digest/拒绝项，不能猜测缺失 scope。
3. **Projector dual-write**：同一已提交 Journal fact 同时投影现有字段与 V2 scope/route 字段；事务失败回退 checkpoint，不影响 C0--C7。
4. **一致性核验**：逐 scope 对订单、执行、成交、资产、持仓、checkpoint 做行数、主键、金额、quantity、source fact hash 与 watermark 比对；RLS/security-invoker views 必须按 scope 授权。
5. **切换与约束收紧**：新查询 API 读取 V2 view；仅在所有 backfill/replay 通过后将新列设为 `NOT NULL`，建立新的唯一/FK 约束。旧 V1 key/视图在保留期内只读保留。
6. **回退**：切回 V1 read view 和旧 projector version；不删除 V2 数据、不回滚 Journal、不改变已发订单的 route。正式清理必须另立 retention 变更。

对于会改变 `accounts` 主键的最终迁移，优先在 `DISARMED` 维护窗口创建 `*_v2` 表、从 Journal replay、校验后原子切换 view，而不是对活动表执行破坏性主键重写。

## 7. 研发与验收顺序

### H0：券商与市场事实冻结

在编码前获得目标港股券商的官方 SDK/协议、模拟盘或隔离账户，并 probe：支持市场、交易币种、lot/odd-lot、tick、订单类型、交易日历、费用、融资/卖空、rate limit、回调字段、分页、回放与恢复查询。任一未证实能力在 manifest 中显式关闭。

### H1：OMS 通用 scope 与多币种改造

完成 `ExecutionScopeKeyV1`、`MarketRouteV1`、`InstrumentKeyV1`、多币种 `RiskSnapshot`/reservation、scope-local OrderBook/router/guard/kill-switch，以及 Journal schema 的向后兼容扩展。此阶段不连接港股 SDK。

### H2：PostgreSQL V2 与 projector

执行第 6 节迁移、RLS/view、dual projection、replay/backfill 与 rollback。数据库不可用、scope/route/hash 不一致或 projector lag 只能影响 read model/stale 状态；绝不让数据库阻塞 C0--C7。

### H3：港股 EMS Adapter

按最终券商实现独立 profile、mapping、session、callback ingress、query/recovery normalizer 和 route token。任何 SDK callback、native object、密钥、账户、订单载荷均不可跨 Adapter 合同泄漏。

### H4：统一验收

代码候选冻结后一次性完成：

| 类别 | 必须验证 |
| --- | --- |
| 静态/回归 | Ruff、格式、Pyright、全量 pytest、schema/migration contract、旧 EMT 回归。 |
| OMS 隔离 | CNY/HKD 资金不交叉；相同 symbol 不同 market 不交叉；错误 route/币种/rule/过期规则全部拒绝。 |
| EMS/Adapter | 下单、撤单、订单/成交/撤单错误回调、分页查询、限流、断线、SDK/manifest 漂移、scope collision。 |
| 恢复 | 进程强杀边界、callback gap、未知外部单、同账户多 scope、权威查询、manual-only rehydrate、零自动重报/撤单。 |
| PostgreSQL | migration/backfill/replay 幂等、RLS 越权、V1/V2 一致性、checkpoint 原子性、stale/rollback。 |
| 性能 | A 股现有 100 单笔、499 burst、W7 必须在新 revision 重跑；港股按官方 rate limit 设置独立基线，不能把 EMT 499 额度套用到港股券商。 |
| 部署 | 首发 A/HK 分进程、分 Journal、分 scope ownership、分密钥/配置、分 cgroup；一方故障不影响另一方新单准入。 |

## 8. 港股市场规则的实现原则

港交所的 board lot 可由证券发行人决定，odd lot 也具有独立处理方式；证券结算为 T+2。规则存在市场演进，不能硬编码为 A 股固定 100 股或固定 tick。每个 instrument 的可交易数量、价格单位、订单类型、交易时段和结算可卖性应由版本化 rule bundle 决定，并在订单 Journal 中固化其 hash。

官方资料：

- <https://www.hkex.com.hk/Global/Exchange/FAQ/Securities-Market/Trading/Securities-Market-Operations?sc_lang=en>
- <https://www.hkex.com.hk/Services/Trading/Securities/Overview/Trading-Mechanism?sc_lang=en>

## 9. 推荐对外回复结论

> 经评估，TradeCore 不需要拆成 `oms_ashare` 与 `oms_hk` 两套 OMS。现有 OMS/EMS 已具备多 venue、market、currency 的数据模型基础和通用 BrokerAdapter 合同，港股券商的主要新增工作在 EMS Adapter、mapping、session、回调、查询和恢复适配。\
> \
> 但当前 OMS 仍存在 CNY 单币种冻结、按 symbol 聚合持仓、按 account ID 路由的 A 股假设，因此不能只对接券商后直接用于真实港股交易。建议先完成一次中等规模的 OMS 通用化：引入 execution scope、多币种资金冻结、市场化 instrument/position key 与签名 market route；PG read model 做增量 V2 迁移，不拆 A/HK 两套表。\
> \
> 首发部署采用同一 OMS 核心的 A 股/港股独立实例和独立故障域；港股券商确定后新增独立 `HK_<BROKER>` EMS Adapter。整体 OMS 改动量为中等，EMS 接入量取决于最终券商 SDK 和恢复能力，不应假定 EMT 当前已支持港股本地交易。

## 10. 不可跨越的边界

1. 不把港股通查询接口当作港股本地交易 API。
2. 不在 EMT mapping 中新增未经官方证实的港股 native code。
3. 不让策略直接指定券商、session 或 client scope；只能由签名 `MarketRoutePlan` 选择。
4. 不做隐式 CNY/HKD 换汇、跨券商自动 failover、跨市场自动撤单或自动恢复。
5. 不以 PostgreSQL、行情服务、FX 查询、规则下载或 UI 等待作为 C0--C7 同步依赖。
6. 不以港股模拟盘结果替代 EMT、A 股或其他券商的验收证据，反之亦然。
