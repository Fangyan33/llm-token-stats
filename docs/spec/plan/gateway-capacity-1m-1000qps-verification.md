# 1M 混合上下文与 1000 QPS 本地验收记录

本文件保留旧目标的历史结果。用户随后将当前验收标准调整为单实例 200 QPS，输入为 20% 1M、50% 200K、20% 500K、10% 100K；当前标准见[容量测试说明](../../collection-capacity-test.md)，不据新标准重判本文件的旧负载。

日期：2026-11-11。结论：**本轮容量验收未通过，尚不能确认“1M 上下文＋单 Pod 1000 QPS＋额外 RSS ≤2048 MiB”同时成立。** 无插件组本身已无法维持目标接收速率；启用组还有旁路超时及 Producer 丢弃。中止前的 RSS 差值不能替代完整容量验收。

## 目标与环境

按用户确认的分布：20% 1M、40% 160k、20% 180k、20% 200k。所有请求为 OpenAI 兼容 SSE，服务端在最后返回 usage；插件不读取或改写请求正文。目标为每组 1000 QPS、预热 60 秒、稳定 300 秒，每条流 60 秒，共 360000 请求，结束后排空、精确核对 Doris。

两组共用真实 Envoy 1.22.7-dev 镜像、8 Worker、64 条客户端 HTTP/2 连接；无插件组完全不安装 Wasm，启用组使用当前构建。用户生产的 8 个副本没有被解释为 8 Worker，生产 Worker 数仍未知。HTTP/2 每流窗口 64 KiB、连接窗口 16 MiB；这些仅为本地共同测试参数。采样读取 Envoy 进程 VmRSS，覆盖 Wasm 与 native 状态。

本次匹配对照期间本机为 8 vCPU、12125.7 MiB 内存，压测端、fixture、网关、Observer 和 Kafka 共用该主机，原有 kind 等资源保留。它不是生产单 Pod CPU limit 15c 的等规格环境。较早诊断尝试开始时主机约 7.7 GiB；本次报告采用匹配两组实际记录的内存值。

输入内容为 ` word` 重复 N 次，已用固定 tiktoken 0.12.0 的 cl100k_base 离线确认四档分别为 160000、180000、200000、1000000 token，并保存正文 UTF-8 长度与摘要。1M 内容为 5000000 字节，序列化请求为 5000125 字节。此验证覆盖输入内容，不包括真实模型的聊天模板或推理。模型 usage 和输出用量仍由合成服务报告。

该分布平均输入内容 340000 token、约 1.7 MB/请求，目标正文入站速率约 1.7 GB/s（13.6 Gbit/s），每组输入约 612 GB。没有压缩、截断输入或用头部代替实际上传。

## 容量匹配对照

运行 ID：`518efad8bd5b`。

```sh
python3 scripts/collection_capacity.py --workers 8 --connections 64 \
  --output .tmp/capacity-recheck/matched
```

| 指标 | 无插件 | 启用插件 |
| --- | ---: | ---: |
| 目标请求数 | 360000 | 360000 |
| 最后检查点已发数，非完整最终总数 | 127827 | 103110 |
| 已保存且 HTTP 200、usage、DONE 完整的业务响应 | 5069 | 2470 |
| 其中 1M 输入的完整业务响应 | 68 | 2 |
| fixture 接收完正文数 | 5942 | 3192 |
| fixture 最高单秒接收数，非稳定 QPS | 252 | 146 |
| 活跃 SSE 峰值 | 4976 | 2786 |
| RSS 采样峰值 | 2215.0 MiB | 3066.4 MiB |
| 采样总时长，含强制退出后的样本 | 143.5 秒 | 117.7 秒 |
| 最低主机可用内存 | 510.8 MiB | 422.6 MiB |
| 停止原因 | 主机内存保护 | 主机内存保护 |
| 完整负载验收 | 未通过 | 未通过 |

两组均在主机可用内存低于 512 MiB 时取消负载。取消后 10 秒未退出，工具强制结束压测进程，因此 `finished=false`，只保留周期检查点及已写出的请求结果。数万未完成请求主要堆积于上传；客户端调度 1000 QPS 不等于模型端接收 1000 QPS。未完成规定的稳定时段，也没有有效的排空 RSS，不能把退出后的 RSS 当作业务全部完成后的平台值。

采样峰值之差为 851.4 MiB，按经过时间对齐的最大差为 1134.8 MiB。这些是未达目标、不同实际接收量下的局部样本；**2048 MiB 目标预算尚未完成有效验收**。原始报告中 `rss_within_budget=true` 仅表示局部样本比较，整轮 `passed=false`、两组 `capacity_load_valid=false`。测试工具随后将负载无效时的预算结论改为 `null`，另保留样本比较字段，原始报告不做覆盖修改。

## 已确认的采集损失与对账边界

停止网关前的快照中，Wasm `metering_dropped_timeout_total=63434`，Observer `metering_producer_dropped_total=38751`。Producer 待发队列容量为 256。上述是停止前的快照计数，不能当作最终全量丢失请求数；Producer 明确丢弃事件，已证明本轮没有实现完整采集。没有调大队列、放宽 ACK 超时或把采集丢失当作上游未返回 usage。

`metering_dropped_closed_total=17738` 另作诊断：当前代码收到 Observer 的正常 Close ACK 也累计该计数，后面的 100 请求正确性测试中该值同样为 100，因此不把它单独认定为丢失观察数。超时与 Producer 明确丢弃分别判断。

随后实际 Consumer/Doris 已执行逐 ID 验证，但 180 秒排空窗口结束时 Kafka 仍有积压，报告 `kafka_drained=false`。在当时可见的数据中，2470 个已保存的成功业务响应有 2316 个未满足两类事件各一条；这包含尚未消费的记录，**不能据此宣称最终丢失率为 2316/2470**。入库验证判为失败，没有把尚未排空的计数包装为全量精确对账通过。

身份映射在合成环境中未实现，request_started 的 `identity_invalid` 属于该 fixture 的已知身份边界；超时、旁路关闭及 Producer 丢弃分别记录，不用身份异常掩盖采集损失。

## 独立大上下文正确性验证：通过

为单独确认 1M 正文与 60 秒流的采集行为，另执行 100 请求、并发 100，仍为 20% 1M、其余 160k–200k。运行 ID `e1dc848bae9e`：

```sh
python3 scripts/collection_capacity.py --mode context --workers 8 --connections 64 \
  --output .tmp/capacity-recheck/context
```

100/100 业务均完整 HTTP 200、usage、DONE；包含 20 个 1M 输入、40 个 160k、20 个 180k、20 个 200k。实际上传 170012500 字节，活跃 SSE 峰值 100，端到端 P50 60.245 秒、P99 60.446 秒。Kafka 三分区提交 lag 均为零，精确验证 100 个 ID 各两类事件各一条，共 200 行：input/output 正确、request_model=null、完整终态、用量已知、无缺行或重复、无 transport_lost/missing_end/parse_limit 等采集丢失异常，全部通过。

此正确性用例的网关 RSS 峰值约 121.1 MiB，只代表并发 100；没有同负载无插件对照，不计算插件增量。报告明确 `capacity_load_valid=false`，不以此替代 1000 QPS 验收。其 `prefix_rows=0` 是排空检查调用前的旧诊断快照，逐 ID 验证及 Kafka offset 已确认最终 200 行；工具随后已改为在排空检查之后重新读取该诊断行数，原始报告保持不变。

原始证据：[大上下文正确性报告](../../../.tmp/capacity-recheck/context/e1dc848bae9e/result.json)、[逐 ID 验证](../../../.tmp/capacity-recheck/context/e1dc848bae9e/mixed-context/verification.json)、[Kafka offset](../../../.tmp/capacity-recheck/context/e1dc848bae9e/mixed-context/kafka-offsets.txt)。

## 证据与工具修正

- [容量原始报告](../../../.tmp/capacity-recheck/matched/518efad8bd5b/result.json)、[汇总分析](../../../.tmp/capacity-recheck/matched/518efad8bd5b/analysis.json)。
- [测试环境与摘要](../../../.tmp/capacity-recheck/matched/518efad8bd5b/environment.json)、[token 核验](../../../.tmp/capacity-recheck/matched/518efad8bd5b/tokenizer-verified.json)。
- [无插件采样](../../../.tmp/capacity-recheck/matched/518efad8bd5b/baseline/rss.jsonl)、[启用插件采样](../../../.tmp/capacity-recheck/matched/518efad8bd5b/enabled/rss.jsonl)。
- [Wasm 指标](../../../.tmp/capacity-recheck/matched/518efad8bd5b/enabled/wasm-stats.txt)、[Observer 指标](../../../.tmp/capacity-recheck/matched/518efad8bd5b/enabled/observer-metrics.txt)、[Kafka offset](../../../.tmp/capacity-recheck/matched/518efad8bd5b/enabled/kafka-offsets.txt)。
- [首次补跑的失败记录](../../../.tmp/capacity-recheck/full/2d1d5fcab4ee/failure.json)：取消后被强制结束，旧工具没有最终 load.json。现已每秒原子写检查点，缺少最终汇总仍保存失败结果并继续另一组。
- [强制结束与取消验证](../../../.tmp/capacity-recheck/checkpoints/result.json)：SIGKILL 保留可解析的未完成检查点，SIGTERM 保存取消及业务错误，均不会判为容量通过。

本轮仅修改测试工具、验收判定及文档，未改动网关或 Observer 的生产采集实现。工具新增断言覆盖未完成检查点、取消、降低目标、实际接收速率不足及活跃流不足，禁止这些情况通过容量验收。测试入口及分词核验方法见[容量测试说明](../../collection-capacity-test.md)。

`make capacity-build`、检查点中断实测、Python 编译检查和 `make test` 通过。后者包含 110 个 Python 测试以及主模块、独立插件模块 Go 竞态测试。没有将此前的 Wasm 构建或 Kubernetes E2E 历史结果当作本轮重新执行的容量验收。所有本轮 Docker 容器与子进程已结束，仅保留诊断文件；既有 kind 和 buildkit 容器保留。未提交或推送代码。
