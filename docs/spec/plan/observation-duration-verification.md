# 采集时长默认 30 分钟、最大 1 小时

日期：2026-11-12。只通过 Wasm 配置采集时长，Observer 保留独立的固定清理兜底。

## 配置与行为

| 项目 | 行为 |
| --- | --- |
| Wasm `observation_max_ms` | 省略时及模板默认 1800000 ms（30 分钟）；显式范围 1–3600000 ms；0、null、负数及超限值拒绝 |
| Observer | 移除 `max_observation_ms` 配置及 Go 配置字段；内部固定 1 小时＋5 秒清理期限 |
| 正常结束、取消、传输失败 | 提前清理，不等待固定期限 |
| 单个观察到期 | 停止该观察，业务继续；不关闭承载其他观察的共享传输 |

Wasm 从请求头进入开始计时，包含输入上传、上游等待和流式响应。Observer 从处理 Start 后开始其固定期限，防止没有 End/Cancel 的观察在仍然存活的共享流中永久占用额度。单观察不新增永久协程或 ticker。

ACK/结束确认的 2 秒上限、16 KiB 分片、8 MiB 累计响应、Pod 观察及字节额度保持原有约束。实际长流会增加同时存活的业务连接和观察状态；本次不据时限调整推断额外 RSS 已满足 2048 MiB，也不替代既定容量验收。

## 升级顺序

旧 Observer ConfigMap 必须删除 `max_observation_ms`，新版本严格拒绝这一未知字段。先更新配置和 Observer 并等待就绪，再更新 Wasm 及 EnvoyFilter，以免仍运行旧版的 Observer 提前在约 10 分钟关闭观察。生产示例和 `make package` 渲染配置已移除该字段，Wasm 示例、本地 Envoy、Schema、组件说明及制品 README 已同步。

现有 `dist/llm-token-stats-package-575ee9db2b59` 是此前构建的制品，不包含本次修改。本轮没有发布或部署远端生产环境。

## 验证

- `make test`：116 个 Python 测试及主模块、插件 Go 竞态测试通过。覆盖默认值、配置上下限、null/零拒绝、Observer 旧字段拒绝、打包配置及模板一致性。
- Wasm 的 30 分钟/1 小时边界使用模拟状态时间验证：期限正确，末分钟仍采集，到期仅清理该观察，同一共享通道的其他观察正常，业务继续。
- Observer 的虚拟时钟覆盖一小时附近的完整 SSE、尾部 usage 和超出 32 位微秒范围的原始时间戳；固定兜底到期时仍保留独立归属查询结果。
- 使用真实打包渲染函数生成配置，确认 Observer 无旧字段，Wasm 默认 1800000 ms；用实际构建的 Observer 镜像及该配置启动，成功进入 `grpc_listening`。这项启动检查隔离网络，不代表 Kafka/Doris 连通性验收。
- `make k8s-test K8S_TEST_ARGS='--context kind-kind'` 完整通过：27/27 项，无失败、无跳过；Wasm 构建、ABI 与宿主缓冲所有权回归通过。实际网关验证包含 7 类 ECDS 加载故障、透传与恢复，以及采集、Kafka/Doris、归属、请求透传和性能事件回归；既有网关保持未变。结果见 [Kubernetes 完整日志](../../../.tmp/k8s-collection/kind-kind/metering-wasm-verify/e412c9213ec0/integration.log)、[汇总](../../../.tmp/observation-duration/k8s-summary.json)。

单元测试中的长时边界使用虚拟时钟或模拟观察年龄，没有真实等待 30 分钟或 1 小时，也未执行对应时长的容量负载。完整 Kubernetes 回归使用实际短流量；不得将其或边界测试称为一小时容量验收。VM 崩溃、Envoy 进程故障及生产资源组合未获穷尽验证，不将 fail_open 开关等同于全阶段保证。

本地原始日志保存在 `.tmp/observation-duration/`，包括完整单元/竞态日志、Wasm 构建与 Kubernetes 日志、专项边界回归及打包模板启动日志。Wasm SHA-256：`db4a6c50f8ec3cf24fb427f2b62f5fca270421fa78cc1b890831d358cd1550f6`。
