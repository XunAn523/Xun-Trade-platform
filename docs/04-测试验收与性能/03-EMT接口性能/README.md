# EMT 接口性能文档

本目录定义 EMT 交易网关的**性能测量与验收合同**，覆盖 [接口全覆盖矩阵](../../02-接口与数据/02-EMT交易与账户接口全覆盖矩阵.md)中的 86 个 `TraderApi` 方法和 69 个 `TraderSpi` 回调。它定义测量边界、必须采集的直方图、负载模型和发布门禁；不把网页说明或开发机结果伪装为柜台的实盘性能承诺。

| 文档 | 覆盖范围 |
| --- | --- |
| [交易接口性能文档](./交易接口性能/README.md) | 86 个主动 API 的调用、排队、SDK 返回与异步确认性能。 |
| [回调接口性能文档](./回调接口性能/README.md) | 69 个 SPI callback 的入队、持久化、归一化与策略 ACK 性能。 |
| [全接口压测建设方案](./01-全接口压测建设方案.md) | 86/69 场景矩阵、C0–C7 关联追踪、资源归因、实测步骤与发布门禁。 |
| [Kungfu参考低延迟优化方案](./06-Kungfu参考低延迟优化方案.md) | 以固定账户 lane、预构建路由、回报直达与账本旁路为参考的 TradeCore 分阶段优化、验收与回滚规则。 |
| [Python OMS/EMS 综合热路径优化方案](../04-OMS性能与容量/01-Python-OMS-EMS综合热路径优化方案.md) | 综合 Kungfu、WonderTrader 和 vn.py 的分层设计；明确热路径、OMS 投影、EMS 边界、优化阶段与 499 笔验收门禁。 |
| [OMS/EMS 旁路能力优化实施方案](../04-OMS性能与容量/02-OMS-EMS旁路能力优化实施方案.md) | 定档 Journal replay、订单/成交状态机、EMS control plane、OMS immutable snapshot 及 callback direct-write A/B；明确它们不得反向阻塞 C0--C7。 |

可执行的 Linux 压测骨架位于 [`tools/performance/`](../../../tools/performance/README.md)。它以同一份 86/69 catalog 强制报告覆盖完整性；synthetic driver 只验证网关自身开销和测试链路，真实 EMT 指标必须由 Linux 测试账户环境和经批准的 native driver 生成。

## 共同测量规则

所有时间使用单调时钟 `perf_counter_ns()`，结果采用 HDR Histogram 或等价的可合并直方图，至少报告 `count`、`min`、`p50`、`p95`、`p99`、`p99.9`、`max`、错误数、拒绝数、队列水位和流控数。禁止仅报平均值。

| 标记 | 含义 |
| --- | --- |
| `C0` | 策略命令或原生 callback 进入网关。 |
| `C1` | 鉴权、幂等、风险及 admission 完成。 |
| `C2` | Command/Event Journal 已 durable。 |
| `C3` | EMT Python binding 调用开始。 |
| `C4` | binding 从主动调用返回。 |
| `C5` | SPI callback 的最小副本已进入 ring。 |
| `C6` | 归一化事件已 durable。 |
| `C7` | 策略客户端 ACK 已发送。 |

正式性能预算写入版本化 `performance-baseline.json`，并与 capability report、SDK 哈希、Python ABI、OS、CPU、内存、磁盘、账户、配置和负载模型一起审批。新版本只能与**相同环境和相同负载模型**的已批准基线比较；环境、SDK 或账户权限变化时必须重新建基线。

所有压测均必须满足：不出现未解释命令/回调、`CALL_STARTED` 后不重发、callback 无同步 I/O、无 ring 溢出、审计链连续、告警链可追溯，并且实际发射速率不高于已实测的柜台安全速率。任何缺口即使延迟更低也判定失败。
