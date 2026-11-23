# HyperPerf 接入暂停存档

存档日期：2026-10-31。根据用户要求，暂停 TradeCore 压测接入的后续设计，待 HyperPerf
下一阶段开发完成后再继续。本次只记录状态，不实施桥接、不部署、不执行压测。

## 已完成的基线

`dev` 已提交并推送 `d3e587cd484a70e524fb246b6c1010494a539a15`，包含
[HTTP 联通脚本](http_smoke/hyperperf_http_smoke.py)、
[操作名称清单](http_smoke/operation_names.json)、
[配置与验收说明](http_smoke/README.md) 和离线合同测试。
该提交的 44 项离线测试、语法及 Ruff 检查通过；实际 HyperPerf 依赖加载、网络冒烟和
服务器压测不属于这项离线通过结论。

脚本只测 HyperPerf 隔离靶的 `GET /work`，严格校验 HTTP 200 和 `{"accepted":true}`。
它不调用 TradeCore，因此不构成 TradeCore 吞吐、延时或资源用量的验收。

## 暂停范围

- TradeCore 独立 OMS、EMS/策略 HTTP 性能入口的后续脚本与目标配置。
- Consumer 原生 UDS 的 HyperPerf Runner 接入。
- HTTP 到 UDS 的受控测试桥。这只是讨论选项，不是已批准的实现路线。
- TradeCore 目标进程、线程、cgroup 与发生器的监控绑定和联合验收。

不自动创建计划、升压、启动服务、执行模拟委托或跟踪 HyperPerf 开发状态。
现有脚本与运行服务保持原状。

## 恢复时核对的条件

用户确认继续后，先核对 HyperPerf 新阶段的实际部署版本、协议合同和客户端能力，
再确定具体接入方式，不沿用当前讨论直接实施。

1. 确认原生 UDS 是否获得平台正式支持，包括 socket 范围、隔离策略、Linux 身份授权、
   请求关联、超时、停止、计数和原始产物；未经批准不得直接访问宿主机 socket。
2. 确认 TradeCore 实际目标、HTTP origin 或 UDS 路径与 fixture。现有 `/work` 示例靶
   不能替代 TradeCore；本地高吞吐测试优先使用不调用 EMT 的 Consumer 配置。
3. 将监控绑定到真正承受负载的进程，并区分 TradeCore、桥接（如采用）和发生器。
   核对 systemd 选择器、PID 启动身份、子进程范围、权限和采集缺口。
4. 重新固定业务成功边界与计划。此前讨论的上限为合计 2,000 并发、2,000 次发起/秒，
   不等于自动获得运行或升压授权；EMT 场地速率限制另行保留。
5. 先低负载验证，再安排性能测试。独立 HTTP、原生 UDS、桥接端到端和 EMT 执行结果
   分开记录；资源采集缺失不能补零，微秒单位不能视为微秒级测量准确度。

当前没有下一阶段压测或部署通过结论。
