# TradeCore 项目汇报图片

本目录保存 [`TradeCore 完整技术与性能汇报`](../../05-TradeCore完整技术与性能汇报.md) 中直接引用的 SVG 矢量图。SVG 适用于 Markdown、PPT、PDF 和网页汇报，可无损放大；不含账户、订单、证券、地址或凭据。

| 文件 | 主报告位置 | 内容 |
| --- | --- | --- |
| [01-账户准入状态机.svg](./01-账户准入状态机.svg) | 2.3 | `BOOTSTRAP`、`RECOVERING`、`DISARMED`、`READY` 的准入边界。 |
| [02-TradeCore总体架构.svg](./02-TradeCore总体架构.svg) | 3.1 | 策略、EMS、执行热路径、Journal、OMS sidecar、查询和可观测性的总体边界。 |
| [03-策略信号到下单数据流.svg](./03-策略信号到下单数据流.svg) | 4.1 | 策略信号从冻结指令到 C2--C7 事实交付的完整数据流。 |
| [04-策略到回报时序图.svg](./04-策略到回报时序图.svg) | 4.2 | Strategy、EMS、Ingress、native lane、EMT SDK 与 callback/OMS 的时序关系。 |
| [05-账户路由与会话分片.svg](./05-账户路由与会话分片.svg) | 5.3 | account→binding→独立 scope/session/native lane 的静态路由模型。 |
| [06-OMS核心类关系图.svg](./06-OMS核心类关系图.svg) | 7.2 | 执行合同、Adapter、事实、投影、快照与柜台订单标识的 UML 类关系。 |
| [07-订单状态机.svg](./07-订单状态机.svg) | 7.3 | 下单、受理、回报、撤单与不确定副作用后的对账状态机。 |
| [08-OMS查询模型ER图.svg](./08-OMS查询模型ER图.svg) | 8.3 | OMS PostgreSQL 查询模型中账户、订单、执行、动作、成交与检查点的关系。 |

## 维护约定

- 主报告内相邻的 Mermaid 代码块是每张图的可编辑源；修改图前先更新该源码，再重新渲染对应 SVG。
- 本次以 Mermaid CLI `11.17.0`、白色背景生成。SVG 仅为文档资产，不参与下单、回调、OMS 投影或性能热路径。
- 视觉契约：白底、黑色文字和边框、无彩色填充；流程、状态、类关系与 ER 图使用 ELK + `stepBefore` 的直角折线。时序 UML 使用水平消息线，取消自调用弧线。
- 提交前应确认主报告、此索引和 SVG 文件名一致；不要把测试账户、订单号、价格、服务器地址或任何凭据写入图中。
