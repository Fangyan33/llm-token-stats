# 大上下文响应采集实施与验证记录

本文件记录实施时的历史验证。用户随后将当前容量标准调整为单实例 200 QPS，输入为 20% 1M、50% 200K、20% 500K、10% 100K，见[最新容量验收记录](gateway-capacity-200qps-verification.md)。

日期：2026-11-11。仅操作本地 Docker 和 kind-kind 隔离资源，未修改生产网关，未提交或推送代码。

## 实施范围

请求头、请求正文和响应回调均继续业务。Wasm 不读写或解析请求正文，不注入 include_usage，request_model 保存 null；上游缺少 usage 时保存 null/unknown。响应使用 <=16 KiB 分片及原回调时间，SSE、usage 和性能解析均在 Observer。

每 Worker 默认 4 条长期传输流，批次 <=32 KiB/128 帧/10ms，采用独立 capture_id、观察内序号及累计批次 ACK。单观察关闭不会关闭共享传输。Pod 级 SharedData CAS 分块额度为 131072 个观察、256 MiB 待发及未确认载荷、524288 条记录。ACK 时限 2 秒，观察时限 600/605 秒。native 取消失败保留额度并重试，不能确认回收的额度不提前归还。

Observer 移除请求解析、累计 100ms 墙钟限制和 16 倍累计预留，改为按保留响应缓冲计账，并释放完成的大事件缓冲。Kafka/Doris 事件契约保持不变；内部 Wasm–Observer 协议和配置直接替换，必须配套更新。

## 已通过的验证

- make test：契约一致性、106 个 Python 测试、主模块与独立插件 Go 竞态测试通过。
- make wasm：TinyGo 构建、ABI 及禁止 socket/filesystem/process-exit 导入检查通过。
- 共享传输测试：600 条观察复用 4 条连接、累计 ACK 释放、单观察拒绝/解析失败/关闭隔离、迟到帧、传输超时、共享额度及取消失败保守回收。
- 请求透传：真实 Envoy 的 HTTP/1、HTTP/2 超过旧 256 KiB 上限、32 MiB、无效 JSON、trailers 和慢上传；慢上传前缀约 1ms 到达上游。请求正文读写/恢复 hostcall 在原生故障注入中未被调用。

## 100 请求/并发 2 的真实链路回归

实际路径为客户端 → Envoy 1.22.7-dev → 合成 SSE 服务，以及 Wasm → 实际 Observer Producer → Kafka → 实际 Consumer → Doris。网关为 2 Worker，客户端 8 条独立 HTTP/2 连接，总业务并发限制为 2。模型用量为合成 reported 值，不是实际模型 tokenizer 结果。

| 场景 | 请求数 | HTTP/SSE 与 Doris 对账 |
| --- | ---: | --- |
| 16K 输入/1K 输出，正文 82045 字节 | 100 | 全部成功、用量正确、无缺行或重复 |
| 正文 33554555 字节，超过 32 MiB | 4 | 全部成功并完整采集；未设置请求正文采集上限 |
| 65 秒流 | 2 | 全部成功并完整采集 |
| 上游主动省略 usage | 10 | 全部 null/unknown、终态完整，无采集丢失异常 |

116 个观察各有两类事件，共 232 行。对账还校验 request_model=null、正确 response_model、terminal_observed、observation_status、usage_eligible 及异常码。

小并发对照中，裸 Envoy RSS 峰值约 49.4 MiB，启用插件约 100.7 MiB，额外峰值 51.3 MiB。该数据只代表本轮并发 2，不能用于宣称 1000 QPS 或生产 2048 MiB 预算达标。

原始报告：[回归 result.json](../../../.tmp/collection-capacity/f7f60c8ad884/result.json)。

## 1000 QPS 容量尝试：2 Worker

目标未降低：每组开环 1000 QPS、60 秒 SSE、预热 60 秒、稳定 300 秒、20% 合成 1M 输入，其余 160k–200k。每组目标 360000 请求；真实序列化输入约 0.8–5 MB/条。

本机只有 8 CPU、约 7.7 GiB 内存。两组都在可用内存低于 512 MiB 时停止：无插件组发起 24492 条，启用组 12609 条；均未进入稳定阶段，未完成 60 秒业务响应，因此容量判定失败。无插件组自身 RSS 峰值已达 4160.7 MiB，启用组约 4720.7 MiB。早期按时间对齐的最大差约 634.8 MiB，不能替代完整稳定阶段的 2048 MiB 预算验收。

失败阶段另观察到 Observer Producer 的既有 256 条 pending 队列丢弃 1550 条事件。已记录缺失/不完整采集，没有把它们归为服务端省略 usage；本次未改变 Producer 投递语义或据此宣称正常负载完整率。

原始报告：[容量失败 result.json](../../../.tmp/collection-capacity/3a9e9280730c/result.json)。保留每请求结果、fixture 实际接收速率、RSS/CPU、日志、指标和入库对账。代码额度不是操作系统 RSS 硬上限，生产容量尚未验收。

## Kubernetes 完整 E2E

此前两处验证问题已定位并修复：fixture 由服务端主动返回 usage 后，旧 422 预期需要改为原文 200；Istio 的 UUID tracing 位可能改变 x-request-id，对账改用独立 logical_request_id 并保存实际网关请求 ID。

第三轮 27 个用例中 25 个通过，ConsumerMaximumBatchLimits 和 ConsumerLongFlushRebalanceAndReplay 在既有 45 秒窗口内未完成，未修改这些断言或超时。清理容量测试资源后，第四轮完整 E2E 在原条件下 27/27 具名通过、无跳过。Consumer 最大批次两个边界和长周期重平衡均通过原断言，未放宽等待窗口。完整记录：[Kubernetes result.json](../../../.tmp/k8s-collection/kind-kind/metering-wasm-verify/08501e2e133c/result.json)、[integration.log](../../../.tmp/k8s-collection/kind-kind/metering-wasm-verify/08501e2e133c/integration.log)。

## 运行中重启 Observer

使用同一个持续运行的网关，10 个请求、并发 2、每条 5 秒 SSE。首批两条流进行中重启 Observer，10/10 业务均完整 HTTP 200、收到 usage 和 DONE。随后 8 个观察在同一网关内恢复采集，Doris 逐 ID 检查各两类事件、正确 reported 用量和完整终态，全部通过。

受重启影响的首批 2 个观察仅有 request_started，未伪造 usage、End 或完整性；它们单独保留为故障漏采证据，不计入正常完整率，也不归为上游未返回 usage。原始记录：[faults result.json](../../../.tmp/collection-capacity/aea6ad850af2/result.json)、[中断观察明细](../../../.tmp/collection-capacity/aea6ad850af2/observer-outage/interrupted-captures.tsv)。执行入口为 make collection-fault-test。

## 最终边界与清理

功能、协议、竞态、真实网关故障回归与低并发精确入库验证已通过。1000 QPS/60 秒流/完整稳定时段/2048 MiB 额外 RSS 的容量目标没有通过本机验收，不能把早期 RSS 差值或 Pod 逻辑额度当作该目标已达成。生产 Worker 数仍未知；本轮实际为本地 2 Worker，8 个生产副本没有被解释为 8 个 Worker。

运行中的 VM 崩溃、Wasm OOM 及 Envoy 自身失效未通过实测覆盖，fail_open 配置不能代替这些边界的实际验证。完整 E2E 的所有具名断言保持原有要求，临时命名空间已通过所有权校验清理；Docker 容器仅清理本轮唯一运行 ID 对应的实例，原始诊断保留。未改动生产环境，未提交或推送代码。
