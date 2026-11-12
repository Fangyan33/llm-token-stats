# 单实例 200 QPS 混合上下文验收

日期：2026-11-11。按用户最新要求，将当前容量标准改为单实例 200 QPS，输入分布为 20% 1M、50% 200K、20% 500K、10% 100K。旧 1000 QPS 报告保留历史结果，不按新标准重判。

**最终结果：未通过。** 两组均发起并保存 72000 个请求结果，但实际稳定接收速率不足、存在业务错误；RSS 最大时间对齐增量为 **2926.918 MiB**，超过 2048 MiB。实际 Consumer/Doris 已排空 Kafka 并完成逐 ID 核验，采集完整性也未通过。判定依据是启用组相对无插件组的增量，不是启用组总 RSS 高于 2 GiB。

## 验收条件

| 项目 | 当前标准 |
| --- | --- |
| 业务负载 | 单实例开环 200 QPS，所有请求为流式 |
| 每条响应时长 | 60 秒 SSE，最后返回 reported usage |
| 输入分布 | 20% 1M、50% 200K、20% 500K、10% 100K |
| 阶段 | 60 秒预热＋300 秒稳定期，随后排空 |
| 请求数 | 每组 72000 个，约 12000 条活跃流 |
| RSS 对照 | 同负载启用插件 RSS 减去不安装插件 RSS ≤2048 MiB；不限制启用组总 RSS 为 2 GiB，增量包含 Wasm 与 Envoy native 状态 |
| 业务完整性 | 全部 HTTP 200、usage、DONE，无客户端业务错误 |
| 采集完整性 | 每个请求 input/output 与终态正确，Doris 两类事件各一条，无缺失或重复 |
| 实际负载门槛 | 稳定期模型端平均接收至少目标 99%，活跃 SSE 峰值至少预期 90%，四档数量严格匹配 |

每组期望 1M 14400 个、200K 36000 个、500K 14400 个、100K 7200 个。脚本的目标、默认值和判定测试已同步修改；199 QPS、更短时段、错误分布、未完成检查点均不能判为当前容量通过。未知 usage 仍保留 null/unknown；本轮正常场景由 fixture 保证返回 usage，不估算或补零。

## 本地环境与正文核验

本地使用 8 vCPU、约 11.8 GiB 内存，8 个 Envoy Worker、64 条独立客户端 HTTP/2 连接；这些是本地设置，生产 Worker 数仍未知。用户的 8 个生产副本没有被解释为 8 个 Worker。本地压测端、fixture、Observer、Kafka 与网关共用主机，生产单 Pod CPU limit 为 15c，因此本地结果不自动代表生产同规格验收。

RSS 明确按增量判定：`ΔRSS(t) = RSS_enabled(t) − RSS_baseline(t)`。基线 12 GiB、启用组 13.5 GiB 时，增量 1.5 GiB，应通过这一预算检查。启用组总量超过 2 GiB 不能单独用作失败依据。已新增测试覆盖该例、恰好增加 2 GiB 的边界和按时间对齐后超额的情况。

输入内容为 ` word` 重复 N 次。已通过 tiktoken 0.12.0 / cl100k_base 离线核验 100K、200K、500K、1M 内容分别为对应 token 数，保存 UTF-8 长度和摘要；此证据覆盖输入内容，不包括真实模型的聊天模板、推理或输出分词。1M 输入的内容为 5000000 字节，序列化请求 5000125 字节；全分布平均输入 410K token、正文约 2.05 MB。目标入站正文约 410 MB/s（3.28 Gbit/s），每组正文约 147.6 GB，均实际上传，没有截断或压缩。

实际路径为客户端 → Envoy 1.22.7-dev → 合成 SSE 服务，以及 Wasm → 实际 Observer Producer → Kafka → 实际 Consumer → Doris。请求正文不进入 Wasm；只旁路发送响应。两组均使用 HTTP/2 每流窗口 64 KiB、连接窗口 16 MiB，fixture 使用 64 KiB 缓冲增量丢弃上传正文。两组负载端均采用 GOMAXPROCS=2、fixture 均为 4，网关 Worker 与 Observer 不受此参数影响。没有修改生产网关配置或采集额度。

当前运行 ID：`9b6019d8313a`。RSS 通过宿主机 `/proc/<Envoy PID>/status` 直接采样，启动时间校验防止 PID 复用；已验证与容器内 `/proc/1/status` 读数一致，避免旧采样器频繁 docker exec 的开销及超时。

```sh
python3 scripts/collection_capacity.py --qps 200 --workers 8 --connections 64 \
  --helper-gomaxprocs 2 --fixture-gomaxprocs 4 --fixture-read-buffer-kib 64 \
  --audit-timeout 900 --output .tmp/capacity-200qps-proc
```

Consumer/Doris 在业务阶段结束后启动并排空，默认等待上限改为 900 秒。这个离线等待窗口独立于业务稳定时长，不放宽吞吐、RSS 或完整性断言。只有 Kafka 三分区提交 lag 为零并逐 ID 核对通过才算全量对账通过。

## 完整对照结果

| 指标 | 不安装插件 | 启用插件 |
| --- | ---: | ---: |
| 已发请求及保存的结果 | 72000 | 72000 |
| 完整 HTTP 200、usage、DONE | 57095 | 26764 |
| 客户端业务错误 | 14905 | 45236 |
| 稳定期实际接收 QPS | 130.074 | 56.970 |
| fixture 接收完正文数 | 64385 | 34468 |
| 活跃 SSE 峰值 | 11689 | 8685 |
| RSS 峰值 | 903.332 MiB | 3818.910 MiB |
| 请求退出后的 RSS | 891.992 MiB | 3818.910 MiB |
| 负载程序总时长，含排空 | 538.948 秒 | 526.848 秒 |
| 主动取消或内存保护中止 | 无 | 无 |
| 目标负载判定 | 未通过 | 未通过 |

两组的已发分布均严格为 1M 14400 个、200K 36000 个、500K 14400 个、100K 7200 个。业务错误包含请求超时，不能将已调度 200 QPS 当作实际成功承载 200 QPS。无插件基线本身也未达到目标，不能用本地结果声称生产同规格容量已完成验收。

### RSS 按增量核算

- 两组峰值之差：3818.910 − 903.332 = **2915.578 MiB**。
- 最大时间对齐增量：启用组第 527.190 秒为 3818.910 MiB，无插件组第 527.143 秒为 891.992 MiB，差值 **2926.918 MiB**，超出预算 **878.918 MiB**。
- 逻辑观察数、待发和未确认记录在负载结束后已归零，网关 RSS 仍约 3819 MiB；逻辑额度归还不代表进程 RSS 自动回落。

这是完整采样中实际观察到的增量超额。同时，两组实际吞吐均未达标，因此报告 `rss_evaluation_valid=false`、`rss_within_budget=null`，不把该数据标成完成了目标负载下的正式 RSS 验收；样本比较独立记为 `sampled_rss_comparison_within_budget=false`，整轮 `passed=false`。没有以启用组总 RSS 超过 2 GiB 作为失败理由。

### Kafka 排空后的精确对账

实际 Consumer 排空并提交三分区 offset，`kafka_drained=true`，Doris 最终可见 **89395 行**。逐 ID 校验全部 72000 个客户端请求，预期两类事件各一条，共 144000 行。核验结果：

- 59567 个请求不满足完整校验，其中包含全部 45236 个业务失败请求。
- 43142 个请求的两类事件数量不符（缺失或重复）；这个数是请求数，不是丢失行数。
- 在 26764 个业务成功请求中，14331 个仍未满足采集校验，12433 个通过。此数由核验失败数减去必然不通过业务条件的请求数得出，未将所有采集异常都归因于客户端超时。
- 停止前 Observer 指标记录 Producer 明确丢弃 **33357 条事件**、成功 **89395 条事件**、pending=0；Wasm 选中 72000 个请求，超时清理计数 50624。事件数和请求数分别报告，不把丢弃事件数直接当作丢失请求数。

本轮已排空，所以这些核验结果不再包含上一轮因尚未消费造成的临时缺行。服务端保证返回 usage 的正常业务请求发生采集缺失时，不作为“上游主动未返回 usage”豁免。`identity_invalid` 来自合成环境未实现身份映射的边界；核验不以它替代 transport_lost、missing_end 等采集损失判断。`metering_dropped_closed_total` 包含正常 Close ACK，不单独作为丢失计数。

### 内存分布与采样边界

结束前对实际 `/usr/local/bin/envoy` 进程保存 smaps、status 和 admin `/memory`。可见 8 块各 512 MiB 的大匿名映射，另有较大的 native heap 保留。只凭这些映射尚不能认定具体对象的泄漏根因，保留诊断供后续定位，没有据此宣称已修复内存增长。

本轮原始 `rss.jsonl` 的诊断字段 `gateway_pid` 曾被循环中的辅助进程 PID 覆盖；RSS、CPU 均在进入该循环前从正确的缓存 Envoy PID 读取，数值不受影响。字段已修复并新增回归测试，历史原始文件不重写。正确 PID 与说明保存在 `gateway-pid-correction.json`；首次误按诊断字段取得的辅助进程映射移入 `loader-memory-diagnostic/` 并标明不可归因 Envoy，重新取得的 `gateway-memory-*` 文件已核对可执行文件为 `/usr/local/bin/envoy`。

## 工具开销诊断

最初的 `89262cabf79c` 使用负载端、fixture 各 8 个 Go 并行线程、8 KiB 读取缓冲。无插件基线进入长流阶段后实际接收速率下降并出现自然超时，故主动中止；取消后新增错误不能当作正常业务错误率，也没有将该轮与不同工具设置的启用组混合对照。

独立短时直连 fixture 诊断，各模式均完成 3000 个新分布请求。64 KiB 缓冲下，Go 并行度 8 与 2 的负载完成时间分别约 9.38、9.39 秒，工具合计 CPU 时间约 51.6、27.2 秒。后者在该短测中节省约 47% CPU 时间，但随后 `2482d8891493` 的 60 秒流诊断发现 fixture 并行度 2 限制了吞吐，因而中止。短测使用更高到达速率和 2 秒 SSE，只作工具对比，不能据此选择正式长流场景的全部参数。数据见[短测诊断](../../../.tmp/capacity-200qps/helper-diagnostics/result.json)。

最终采用负载端并行度 2、fixture 并行度 4，先独立直连验证：开环 200 QPS 持续到达 90 秒、60 秒 SSE，共 18000 请求全部完成、无业务错误；60–90 秒窗口实际接收 200.034 QPS，活跃 SSE 峰值 12024，实际上传 36902250000 字节。此预检确认工具在满长流并发时能够产生目标负载，不包含网关、Observer 或 Doris，不当作采集容量通过。数据见[长流工具预检](../../../.tmp/capacity-200qps/long-helper/result.json)。正式对照两组均采用预检确认后的相同参数。

## 旧采样器中断时的 RSS 增量证据

上一轮 `4787cd20fb05` 在启用组约第 262 秒之后，docker exec 采样命令超过 5 秒，整轮中断，未启动 Doris 对账。该轮不构成完整容量验收，但保留的 RSS 样本可计算增量：启用组第 241.750 秒为 3141.859 MiB，无插件组最接近的第 241.992 秒为 736.871 MiB，增量 **2404.988 MiB**，超过 2048 MiB。两组独立峰值之差为 2154.574 MiB，也超过增量预算。判断依据是两组之差，不是启用组总量超过 2 GiB。

这一轮无插件稳定期实际接收仅 110.589 QPS，启用组也未完成规定时段，因此这些是未达完整目标时的局部超额样本，不能包装为完整 200 QPS 验收结果。原始数据和派生比较见[旧采样器失败记录](../../../.tmp/capacity-200qps-final/4787cd20fb05/failure.json)、[局部 RSS 增量](../../../.tmp/capacity-200qps-final/4787cd20fb05/rss-comparison-partial.json)。当前运行改用直接读取 `/proc`，保持原业务目标、输入和两组参数不变。

## 证据

- [QPS、RSS 后续诊断与优化建议](gateway-performance-diagnosis-and-optimization.md)：区分已经量化的同步开销、堆高水位与尚未归因的 GC 后保留对象，不将诊断构建当作容量验收。
- [完整原始报告](../../../.tmp/capacity-200qps-proc/9b6019d8313a/result.json)、[验收汇总](../../../.tmp/capacity-200qps-proc/9b6019d8313a/acceptance-summary.json)、[RSS 增量及对应时点](../../../.tmp/capacity-200qps-proc/9b6019d8313a/rss-comparison.json)。
- [本轮环境](../../../.tmp/capacity-200qps-proc/9b6019d8313a/environment.json)、[token 核验](../../../.tmp/capacity-200qps-proc/9b6019d8313a/tokenizer-verified.json)。
- [无插件 RSS](../../../.tmp/capacity-200qps-proc/9b6019d8313a/baseline/rss.jsonl)、[启用插件 RSS](../../../.tmp/capacity-200qps-proc/9b6019d8313a/enabled/rss.jsonl)、[Kafka offset](../../../.tmp/capacity-200qps-proc/9b6019d8313a/enabled/kafka-offsets.txt)、[逐 ID 核验](../../../.tmp/capacity-200qps-proc/9b6019d8313a/enabled/verification.json)。
- [Wasm 指标](../../../.tmp/capacity-200qps-proc/9b6019d8313a/enabled/wasm-stats.txt)、[Observer 指标](../../../.tmp/capacity-200qps-proc/9b6019d8313a/enabled/observer-metrics.txt)、[实际网关内存分布](../../../.tmp/capacity-200qps-proc/9b6019d8313a/enabled/gateway-memory-mappings.json)、[诊断 PID 更正说明](../../../.tmp/capacity-200qps-proc/9b6019d8313a/enabled/gateway-pid-correction.json)。
- [采样器实测](../../../.tmp/capacity-recheck/sampler-verification.json)：宿主机与容器内 RSS 读数相等，PID 复用校验生效。
- [当前容量工具及复现说明](../../collection-capacity-test.md)。
- 本轮 `make test` 通过：115 个 Python 测试、主模块和独立插件模块 Go 竞态测试；新增 Go 测试核对负载生成器的四档分布与正文总长度。RSS 测试覆盖总量超过 2 GiB 但增量达标、增量恰好 2 GiB，以及按时间对齐后超额的情况，另验证辅助进程不会覆盖采样对象 PID。

本轮仅更新测试工具、验收标准和文档，未修改生产采集实现；所有本轮 Docker 容器、负载、fixture、Observer 和 Consumer 已结束，原有 kind、buildkit 容器保留。未提交或推送代码。
