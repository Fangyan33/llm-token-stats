# 响应采集回归与容量对照

测试入口只创建带唯一运行 ID 的本地 Docker 容器，使用固定 Envoy 1.22.7-dev 镜像、实际 Observer Producer 和 Kafka。模型为合成 SSE 服务，不调用生产模型；输入正文可用独立工具离线核验 token 数。Kafka 收完事件并停止网关、负载和 Observer 后，再启动实际 Consumer/Doris 对账，以减少本地同时运行的内存需求。结束时只删除本轮容器，原始请求结果、配置、日志和报告保存在 `.tmp/collection-capacity/<运行ID>/`。

```sh
# 100 请求/并发 2，16K 合成输入/1K 合成输出
# 另含超过 32 MiB 的请求、65 秒流和服务端省略 usage
make collection-regression

# 长流过程中重启 Observer：业务继续完整，后续观察恢复并精确入库
make collection-fault-test

# 不安装插件与启用插件两组，使用相同输入、连接协议和 Worker 数
# 开环单网关 200 QPS；20% 1M、50% 200K、20% 500K、10% 100K；60 秒 SSE
# 每组预热 60 秒、稳定 300 秒，随后排空
make capacity-test

# Worker 数是本地测试参数；用户生产的 8 个副本不是 8 个 Worker
make capacity-test CAPACITY_ARGS='--workers 4'

# 独立指定客户端连接数；两组使用相同设置
make capacity-test CAPACITY_ARGS='--workers 8 --connections 64'

# 独立功能验证：100 个混合上下文请求、并发 100、60 秒流及精确入库
# 不作为 200 QPS 容量通过
make capacity-test CAPACITY_ARGS='--mode context --workers 8 --connections 64'
```

工具默认本地 Worker 为 2，客户端连接数默认取 Worker 数的 4 倍且至少为 4；生产 Worker 数仍未知。当前插件不读取请求正文。合成 1M 内容使用 1000000 次 ` word`，约 5 MB 加 JSON 字段；用户最新确认的混合分布为 20% 1M、50% 200K、20% 500K、10% 100K。平均输入 410K token、正文约 2.05 MB，200 QPS 的正文入站速率约 410 MB/s（3.28 Gbit/s），每组 72000 请求约 147.6 GB。压测机、网关和 fixture 均实际传输正文，没有省略或压缩输入以降低负载。32 MiB 用例为大于 32 MiB 的真实序列化请求。

输入内容可通过以下独立工具验证。该依赖仅用于测试，不引入网关或 Observer：

```sh
python3 -m venv .tmp/capacity-tokenizer
.tmp/capacity-tokenizer/bin/pip install -r tests/capacity/requirements.txt
.tmp/capacity-tokenizer/bin/python tests/capacity/verify_token_profiles.py \
  --output .tmp/capacity-tokenizer/tokens.json
```

工具以固定版本 tiktoken 的 `cl100k_base` 检查四档内容恰有对应 token 数，保存 UTF-8 长度和摘要。此证据只覆盖输入内容，不包含真实模型的聊天模板、输出分词或推理性能。fixture 的 usage 仍由合成服务报告，不能将入库用量正确解释为真实模型完成了 1M 推理。

`environment.json` 保存镜像、Wasm 和负载二进制摘要、CPU/内存及全部参数。每个场景的 `load.json`、`requests.jsonl` 保存请求数量、并发峰值、调度延迟、状态、完整 SSE、客户端 usage 和端到端耗时。负载汇总每秒原子更新检查点；等全部已发请求退出后才写出 `finished=true`，此字段不代表业务成功。强制结束时保留 `finished=false`，该检查点中的请求数不作为最终精确总数。没有最终汇总也会保存失败报告并继续执行另一组，不再丢失整轮证据。`fixture.json` 另外保存服务端实际接受速率、上传数量、已上传字节及活跃流峰值，不能用客户端调度速率代替模型实际接收速率。

两组共同设置下游及上游 HTTP/2 每流窗口 64 KiB、连接窗口 16 MiB，允许最多 100000 流；这些是本地对照参数，未修改生产网关。fixture 增量丢弃上传正文，默认单次读取缓冲 64 KiB，不持有完整输入，可用 `--fixture-read-buffer-kib` 在 1–64 KiB 内指定。负载端 Go 并行度默认为 2（`--helper-gomaxprocs`），fixture 默认为 4（`--fixture-gomaxprocs`）；两组各角色的设置保持一致并记录到环境报告，不改变网关 Worker 或 Observer 并行度。短时工具测试不能替代 60 秒长流下的负载端与 fixture 吞吐验证。

`rss.jsonl` 从 Envoy 进程的 `/proc/<pid>/status` 读取 VmRSS，并单独记录 CPU 时间和主机内存余量。启动时通过一次 Docker inspect 定位宿主机 PID，随后直接读取 `/proc` 并校验进程启动时间，避免频繁 docker exec 的开销与超时；已对照验证该值与容器 `/proc/1/status` 一致。`result.json` 同时报告两组峰值之差、按经过时间对齐的最大差值、稳定阶段中位数和排空值。

**2048 MiB 限制针对增量：`ΔRSS(t) = RSS_启用插件(t) − RSS_不安装插件(t)`，不是启用组的 RSS 总量。** 例如基线 12 GiB、启用组 13.5 GiB 时，增量 1.5 GiB，在预算内；启用组总 RSS 高于 2 GiB 本身不构成失败。预算包含插件产生的 Wasm 与 native 开销。逻辑额度不是 RSS；测试也不把容器 working set 当作 RSS。

负载无效时 `rss_evaluation_valid=false`、`rss_within_budget=null`；观测到的局部差值通过 `sampled_rss_comparison_within_budget` 单独保留。早期报告只有 `rss_within_budget` 时，必须连同两组 `capacity_load_valid` 和整轮 `passed` 阅读，不能将局部样本误报为目标预算通过。

每个客户端请求设置独立 `x-client-request-id`，通过 `logical_request_id` 对账；网关 `x_request_id` 仍独立保存。这样 Envoy 对 UUID tracing 标志的改动不会造成测试误判缺行。

Doris 验证每个已保存结果的 ID 恰有两类事件各一条、有效终态、完整观察、request_model=null、正确 response_model、准确 input/output 以及资格。验证前还等待三分区 Kafka 已提交 lag 为零并保留 offset 证据，避免中止测试中大量无客户端结果的 request_started 提前满足行数等待。服务端明确省略 usage 时要求 null/unknown、missing_usage，并拒绝 transport_lost/missing_end 等采集损失混入此场景。业务错误、缺失、重复、未知用量或采集异常都会失败。负载被强制结束、客户端结果不完整时，局部对账通过也不代表全量完整率。

容量通过还要求完整目标流量及四档输入占比、稳定阶段实际接收平均至少目标的 99%、服务端活跃流峰值至少目标并发的 90%，以及 100% 对账和 RSS 增量达标。降低 QPS、预热或稳定时长可以作开发诊断，但参数低于 200/60/300 时不会判定当前容量目标通过。生产同规格容量仍需单独验收，报告不会将本地通过自动标为生产通过。此前 1000 QPS 与旧分布的报告保留为历史结果，不套用新的验收条件重新标记。

Consumer/Doris 在网关停止后排空，默认等待上限为 900 秒，可通过 `--audit-timeout` 指定。该窗口独立于 60 秒预热和 300 秒业务稳定时段；延长离线对账等待不会放宽业务吞吐、完整性或 RSS 的验收要求。若最终仍未排空则失败，不能将未消费记录直接归为丢失。

主机可用内存不足 512 MiB 时，默认停止负载并标记本轮失败，保留原目标、已发请求和样本；这属于本地资源边界，不构成容量通过。该工具不修改既有 kind 资源；容量运行前应先清理已完成的、具有本项目所有权的 Kubernetes E2E 资源，避免同时运行两套 Doris。完整 E2E 使用 `make k8s-test`；源码/竞态检查、Kubernetes 故障回归和容量对照分别记录。

故障用例使用同一个运行中的网关，10 个请求/并发 2/5 秒 SSE。首批 2 个请求中途重启 Observer，要求 10 个业务响应全部完整；受故障影响的 2 个观察允许漏采并单独保存证据，随后 8 个观察必须完整且用量正确。故障豁免不应用于正常回归或容量场景。

## 同一 VM 的多轮内存诊断

```sh
make build capacity-build
python3 scripts/collection_memory_soak.py --workers 8 --connections 64 --waves 3 --qps 100 --seconds 60
```

工具将当前插件复制到独立输出目录，仅在副本中增加堆统计和主动 GC 控制入口；不替换 dist 发布 Wasm，不修改 SDK。三轮使用同一 Envoy 进程及 Worker VM，60 秒响应及四档上下文分布保持不变；每轮结束后再记录各 Worker 的 GC 前后堆、固定批缓冲容量及进程 RSS。100 QPS 为默认归因负载，不代表 200 QPS 容量达标。主动 GC 的 RSS 不参与正式预算判定，正式容量仍使用原始发布 Wasm 的无插件/启用对照。

输出保存每轮请求结果、RSS、Worker 统计和诊断源码；开始前确认没有其他压测占用本机。工具只清理自身带随机标记的容器和进程。
