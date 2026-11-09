# 请求性能指标验证记录

日期：2026-11-09。

## 实现与边界

不估算 token，不新增 tokenizer 依赖或配置。Wasm 为响应片段提供距请求头的回调微秒时间，同回调拆片共用时间；Observer 提取首末有效生成内容时间，Consumer 只保存流式标识与两个时间事实。Doris `event_details` 计算 `ttft_ms`、`tpot_ms`，`requests` 复用并隔离请求冲突，`usage_effective` 透出指标。

TPOT 只依赖上报输出量，不要求输入量已知或 usage_eligible=true；缺少输出量、输出量不超过 1、不完整观测或时间不可信时为 NULL。TTFT 可独立保留；非流式两项为 NULL。开始事件不携带响应时间。时间异常不影响 usage 接纳。当前仍为 schema_version=1，直接更新初始化 SQL，不迁移旧数据。

## 源码与本地检查

- `make generate`：使用已有 `/tmp/request-headers-tools/protoc/bin/protoc` 和同目录 Go 插件重新生成，未手工编辑生成文件。
- `make test`：通过；106 项 Python 测试、主模块及独立 Wasm 模块 Go 竞态测试通过。
- `XDG_CACHE_HOME=/tmp/timing-cache make wasm`：通过 TinyGo 构建与 ABI 检查，未引入 socket、文件系统或进程退出导入。
- Wasm SHA-256：`bedc87dac06528333d4515ae2c554a6ac3af6227c3a5bea4636c098f1a94b194`。
- `git diff --check`：通过。

单元测试覆盖 SSE 任意字节切片与 UTF-8、有效内容类型、角色/心跳/usage/DONE 排除、零耗时、缺失和倒退时间、非流式、断流、开始事件冻结、事件关系校验、同回调拆片时间一致性。新增时间字段只改变 payload_hash，不改变 identity_hash。

## 隔离 Kubernetes 与 Doris

使用 `kind-kind`、Kubernetes 1.22.17、Istio 1.13.5 控制面、测试代理 proxyv2:1.14.6，专用命名空间 `metering-wasm-performance`，run-id `95bb293b3e33`。

诊断目录：`.tmp/k8s-collection/kind-kind/metering-wasm-performance/95bb293b3e33/`。

- 在隔离 Doris 4.1.3 实际创建 17 个数据库对象；读取 information_schema 校验源 DDL 的名称、顺序、类型、可空性和注释，重新生成的数据字典与当前文档完全一致。
- 完成部署采集、消费者故障恢复、网关副本扩容及七类 ECDS 加载故障的真实透传验证：慢下载、404、超时、摘要不匹配、不可达、损坏 Wasm、非法插件配置。
- 首次 `make k8s-test` 未通过：隔离 Doris SQL/Stream Load/可见性查询超时引起多项 45 秒等待失败，总控另遇 Kubernetes API 查询超时。原始结果保留为 `result.json`、`initial-integration.log`；不将此次运行记为通过。
- Doris 恢复后，未修改源码或放宽断言，定向复跑正文超时、新增视图边界及真实网关性能用例全部通过。对应日志保存在诊断目录的 `targeted-recheck.log`；`performance-samples.tsv` 保留实际指标样例。
- 完整复跑 Job `llm-token-stats-performance-verification` 已执行全部 27 项顶层 Go 集成测试：24 项通过、3 项失败、0 项跳过，不能记为完整 E2E 通过。失败项为 `TestConsumerRealTenMinuteIdleFlush`、`TestDSNCredentialsEndToEnd`、`TestBearerPrivacyAndInvalidInputs`，日志出现 Doris 读连接超时/连接失效；没有放宽超时或跳过断言。两项新增性能测试及正文超时回归均通过。完整日志为 `performance-integration.log`，结构化结果为 `performance-result.json`。

指标用例验证首内容 100ms、末内容 300ms、上报输出 5 token 得到 TTFT=100ms、TPOT=50ms/token；同时覆盖仅输出量已知、缺失 usage、0/1 token、非流式、无 End、缺失首/末时间、usage 冲突、请求内容冲突、同回调合并输出及相同事件重放。真实网关用例主动延迟尾部 usage/DONE，验证尾部等待不进入 TPOT，并核对事件 JSON 与 Doris 类型化列一致。

既有 Istio 网关 Pod 的 UID、容器重启次数与测试前一致；前后快照均保存在诊断目录。已执行 `make k8s-clean K8S_TEST_ARGS='--context kind-kind --namespace metering-wasm-performance'` 并确认命名空间已删除；清理后再次核对既有网关 UID 与重启次数未变化。本地诊断保留。

## 三项失败排查与一分钟空闲用例

按用户要求，将 `TestConsumerRealTenMinuteIdleFlush` 改名为 `TestConsumerRealOneMinuteIdleFlush`，配置真实 60000 ms 攒批。保留逐秒检查不得提前写入、到期数据可见和 offset 已提交的断言；整体可见性等待有界为两分钟。不修改生产配置默认值、600000 ms 上限、Doris 连接超时或其他用例断言。

上次三项失败的直接原因与运行环境证据：

- 空闲用例在 `events_raw` 的 `COUNT(*)` 查询处发生 Doris MySQL 读超时，报 `invalid connection`，并未报提前 flush 或到期未写入。该用例记录的单调时钟耗时为 588.92 秒，尚未到原来的十分钟期限。
- DSN 和 Bearer 隐私两项均在 `database(t)` 的初始 `Ping` 阶段发生读超时，驱动重试后返回 `driver: bad connection`；尚未进入账号认证、offset 恢复和凭据隐私等业务断言。
- 同一时段 Kafka 也发生读超时。系统日志在 10:07:21、10:07:28 UTC 记录 `Clock change detected`；VMware Tools 在 10:07:42 UTC 记录采样间隔实际 271 秒、预期 30 秒。空闲用例首尾日志的墙钟跨度约 839 秒，与 Go 报告的单调时钟耗时相差约 250 秒。

这些证据指向虚拟机运行或调度中断及时间校正期间的依赖连接失效，而非三项功能断言不成立。现有证据不能进一步区分宿主机暂停、休眠或调度饥饿；原测试资源已清理，没有保留该时段完整 FE/BE 内部日志，不据此断言 Doris 内部故障根因。

本轮在 `kind-kind` 新建专用命名空间 `metering-wasm-recheck`，run-id `76fc88438c7c`，仅复测上述三项。诊断目录为 `.tmp/k8s-collection/kind-kind/metering-wasm-recheck/76fc88438c7c/`，保存上次失败摘录、主机时钟和 VMware 告警、本次压力采样及实际 Job 清单。

| 本轮用例 | 结果 | 实测 |
| --- | --- | --- |
| `TestConsumerRealOneMinuteIdleFlush` | PASS | 发布后 60.853857575 秒数据可见且 offset 已提交；用例总耗时 65.73 秒 |
| `TestDSNCredentialsEndToEnd` | PASS | 特殊字符密码的 SQL/Stream Load、错误密码未就绪且不提交 offset、修正后恢复；11.23 秒 |
| `TestBearerPrivacyAndInvalidInputs` | PASS | 无效凭据处理、业务响应、事件/Kafka/组件日志隐私断言；3.87 秒 |

真实 Job 成功退出，日志逐项匹配 3 个顶层 PASS，0 FAIL、0 SKIP；结构化结果为 `result.json`，完整日志为 `targeted-integration.log`。未改变连接超时或重试策略。本轮每五秒采样一次主机状态，共 32 次，最大采样间隔 5.014 秒，墙钟与单调时钟的相对偏移变化小于 0.01 ms，没有复现上次的长时间停顿和时钟变化。测试组件重启次数均为 0，既有 Istio 网关 UID 与容器重启次数未变。

`make build k8s-test-build`、`make test`（106 项 Python 测试、主模块与插件竞态测试）及 `git diff --check` 均通过。此次是指定三项的定向复测，未重跑完整 27 项，不覆盖或改写上一轮完整运行的失败记录。

已执行 `make k8s-clean K8S_TEST_ARGS='--context kind-kind --namespace metering-wasm-recheck'`，确认测试命名空间不存在；清理后再次核对既有网关身份和重启次数未变。清理结果与最终快照保存为 `cleanup-result.json`、`existing-gateway-final.json`。

## 未覆盖边界

本地隔离测试不代表生产部署、容量压测或生产 G0–G5 验收完成。网关回调可能合并多个 token，指标是网关可观测口径，不是模型内部逐 token 时延；普通视图修改公式会影响历史查询结果。没有历史迁移、延迟日/月汇总或历史正文重算。
