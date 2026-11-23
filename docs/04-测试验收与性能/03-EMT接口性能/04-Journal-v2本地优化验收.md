# Journal v2 本地优化与验收

## 安全边界

本次仅执行账号无关的 `gateway-local-signal-burst` 驱动。报告均标记为：未加载 EMT SDK、未登录、未联网、未产生委托；不得作为 EMT 或柜台延时证据。

`CALL_STARTED` 仍须在越过 EMT native 调用边界前强制持久化；回调仍须在事件证据 durable 后才 ACK。未使用 Redis，也没有把内存队列作为委托事实源。

## 已完成的实现

- `FileJournal` 使用长期打开的无缓冲 append 句柄，将一批 frame 合并为一次顺序写入后执行 `fsync`。
- 新增每个 durable batch 的 `validation / lock wait / frame / write / flush / total` 分段统计；默认不保存历史，压测驱动才启用有界历史。
- 命令与回调使用独立 journal 文件；突发回调才启用独立 group-commit writer。均匀 300/s 不为不存在的下一条回调引入额外合批等待。
- group commit 支持 `0 ms` 的“立即提交”模式；均匀高频压测默认使用它。真正突发仍可显式配置 0.1 ms 合批窗口。
- 混合命令批中，`CALL_STARTED` 优先于其他命令结果记录，但每批保留至少四分之一容量给普通结果，避免结果记录长期饥饿。
- callback 在 C5 时间戳已写入 trace 后立即唤醒 durable consumer；后置深度指标与生命周期观察器，缩短 C5 到 C6 的调度尾延时。

## 本机验收结果

| 工况 | 样本 | C0→C4 p99 | C5→C7 p99 | 完整性 |
| --- | ---: | ---: | ---: | --- |
| 均匀 1 Hz，30 秒 | 30 | 5.123 ms | 1.046 ms | 30/30，无溢出 |
| 均匀 300/s，10 秒 | 3,000 | 1.771 ms | 1.182 ms | 3,000/3,000，无溢出 |
| 同时突发 300 条，10 轮 | 3,000 | 18.462 ms | 15.622 ms | 3,000/3,000，无溢出 |

对应原始报告：

- `artifacts/performance/journal-v2-final-1hz/report.json`
- `artifacts/performance/journal-v2-final-uniform-300qps/report.json`
- `artifacts/performance/journal-v2-final-burst-300qps/report.json`

## 1 Hz 硬门禁结论

未通过，故默认 `gateway-controlled-order` 仍会在 SDK 加载、登录和 EMT 委托前拒绝执行。经明确授权的 EMT 仿真接口验证可使用 `--skip-local-latency-gate` 绕过默认门槛；该报告必须标记为绕过，且不能作为 1 ms p99 或生产准入证据。

| 本地段 | p99 | 1 ms 门槛 |
| --- | ---: | --- |
| C0→C1 | 0.021 ms | 通过 |
| C1→C2 | 5.093 ms | 未通过 |
| C2→C3 | 0.006 ms | 通过 |
| C3→C4 | 0.006 ms | 通过 |
| C5→C6 | 1.014 ms | 未通过 |
| C6→C7 | 0.057 ms | 通过 |

分段 journal 指标显示 C1→C2 的 command `flush` p99 为 4.696 ms，而编码、锁等待与 write 均明显更小。这是当前主机/NTFS/SSD 的强制刷盘尾延时，不应通过提前发单、异步落盘或提前 ACK 掩盖。

下一步必须在专用、低尾延时且带掉电保护的 NVMe 与隔离 I/O 的主机上复测相同门禁；只有主机门禁通过后，受控真实 EMT 委托结果才可用于低尾延时验收。
