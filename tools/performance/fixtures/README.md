# 受控 EMT fixture 示例

`emt-controlled-order-fixture.example.json` 是受版本控制的安全上限基线。执行前复制到被忽略的本地 `data/performance/fixtures/emt/` 目录，保留 `armed=false` 先做 preflight；只有经过人工审核后，才可在本地副本中设置 `armed=true`。

CLI 没有也不会提供 `--arm` 参数。账户 profile、标的白名单、价格区间、单笔数量、总金额/总数量、命令间隔与撤单超时均由本地 fixture 决定。
