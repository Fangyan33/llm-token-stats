# Consumer 大批次与十分钟攒批验收

日期：2026-11-09。范围：Consumer 配置、批处理调度、分区移交、Doris 可见性确认与隔离 Kubernetes 回归。

## 最终配置

| 配置项 | 默认值 | 最大允许值 |
| --- | ---: | ---: |
| `flush_interval_ms` | 15000 | 600000 |
| `max_batch_rows` | 5000 | 20000 |
| `max_batch_bytes` | 4194304 | 16777216 |

16 MiB 按 Kafka key/value 字节计量。每分区按时间、条数或字节条件先到者刷新；后续消息不重置期限。分区撤销不强制写小批次，未提交数据由新 owner 重放。正常保留的分区不重新初始化。初次审计及分钟巡检不等待攒批清空。

为使 20000 条批次在既有处理预算内完成，Doris 可见性回查提取组内共同键条件；只有一列变化时使用 `IN`，多列变化时保留对应关系。仍每 128 条查询、核对全部唯一键，批次凭据继续核对 next_offset、通知数量及摘要；不放宽导入过滤率或数据库超时。

## 验证结果

- `make test build`：通过，包含契约、105 项 Python 测试、主模块和独立 Wasm 插件的 Go 竞态测试，以及服务构建。
- `go vet ./...`、`git diff --check`：通过。
- 单元测试覆盖三项上限同时使用、默认值不变、越界值、600000 ms 到期与独立分区期限、跨 poll 的 20000 条和 16 MiB、冻结批次重试、取消后的排队记录、空闲到期写入与撤销并发，以及 Kafka fetch 等待解耦。
- 最大批次专项连续两次通过后，对最终代码执行完整命令：`make k8s-test K8S_TEST_ARGS='--context kind-kind --namespace metering-wasm-batch-limits'`。
- 完整 Kubernetes 验收通过：25 项顶层 Go 集成测试全部具名 PASS，共 76 条 PASS，零 FAIL、零 SKIP；部署、扩容及 ECDS 故障透传检查同时通过。

最终完整运行中的新增场景：

| 场景 | 结果 |
| --- | --- |
| `TestConsumerMaximumBatchLimits/20000_rows` | 10.17 秒通过，20000 条合成拒收记录由单批凭据覆盖，Doris 可见后提交 offset |
| `TestConsumerMaximumBatchLimits/16_MiB_valid_events` | 6.91 秒通过，512 条有效事件的 key/value 合计恰为 16 MiB，单批可见并提交 |
| `TestConsumerLongFlushRebalanceAndReplay` | 16.29 秒通过，长攒批期间完成分区移交且不提前写出，退出后重放无丢失或重复 |
| `TestConsumerRealTenMinuteIdleFlush` | 真实等待，逐秒检查没有提前写入；发布后 10 分 0.838842693 秒数据可见并完成 offset 提交 |

环境：kind Kubernetes 1.22.17、Istio 1.13.5 控制面、proxyv2:1.14.6 数据面、Kafka 3.9.2、Doris all-in-one 4.1.3。有效事件样例的时间调整至当前日期，避免将缺少历史分区误判为批次故障。

## 诊断与清理

最终 run_id：`d568c629f96c`。完整结果及具名日志保存在仓库本地目录 `.tmp/k8s-collection/kind-kind/metering-wasm-batch-limits/d568c629f96c/`，其中 `result.json` 为 passed，`integration.log` 记录全部测试。

核对命名空间归属标签及 run_id 后执行 `make k8s-clean K8S_TEST_ARGS='--context kind-kind --namespace metering-wasm-batch-limits'`，成功删除测试命名空间及临时 Kafka/Doris；查询确认命名空间不存在，本地 state.json 为 cleaned=true，诊断文件保留。

上述证据属于源码检查、竞态测试及隔离 Kubernetes E2E；未执行生产环境的吞吐、全分区最大缓冲内存或 G0–G5 验收。16 MiB 是每分区记录预算，资源配置需另计冻结对象、展开正文及客户端缓冲。
